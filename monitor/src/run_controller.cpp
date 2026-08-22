#include "blackbox/run_controller.h"

#include "blackbox/backend_adapter.h"
#include "blackbox/benchmark_adapter.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iterator>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

namespace blackbox {
namespace {

using Json = nlohmann::json;

constexpr std::uint64_t max_json_integer = 9007199254740991ULL;

enum class LifecycleState {
  Starting,
  StartFenceSeen,
  Bootstrap,
  LoadFinishedSeen,
  Running,
  Draining,
  Reconciled,
  Final,
};

[[nodiscard]] std::string_view toString(const LifecycleState state) noexcept {
  switch (state) {
  case LifecycleState::Starting:
    return "STARTING";
  case LifecycleState::StartFenceSeen:
    return "START_FENCE_SEEN";
  case LifecycleState::Bootstrap:
    return "BOOTSTRAP";
  case LifecycleState::LoadFinishedSeen:
    return "LOAD_FINISHED_SEEN";
  case LifecycleState::Running:
    return "RUNNING";
  case LifecycleState::Draining:
    return "DRAINING";
  case LifecycleState::Reconciled:
    return "RECONCILED";
  case LifecycleState::Final:
    return "FINAL";
  }
  std::abort();
}

[[nodiscard]] bool isErrorCode(const std::string_view value) {
  if (value.empty() || value.size() > 96U || value.front() < 'A' ||
      value.front() > 'Z') {
    return false;
  }
  return std::all_of(value.begin(), value.end(), [](const char character) {
    return (character >= 'A' && character <= 'Z') ||
           (character >= '0' && character <= '9') || character == '_';
  });
}

[[nodiscard]] std::string boundedMessage(std::string message,
                                         const std::size_t maximum) {
  if (message.empty()) {
    message = "unspecified monitor failure";
  }
  if (message.size() > maximum) {
    message.resize(maximum);
  }
  return message;
}

[[nodiscard]] std::uint64_t counter(const std::size_t value) {
  if (value > max_json_integer) {
    throw MonitorError(MonitorErrorCode::ResourceExhausted,
                       "monitor counter exceeds the JSON-safe range");
  }
  return static_cast<std::uint64_t>(value);
}

[[nodiscard]] std::vector<std::string>
sortedStrings(std::vector<std::string> values) {
  std::sort(values.begin(), values.end());
  return values;
}

[[nodiscard]] const BenchmarkAdapter &
validatedBenchmarkAdapter(const RunManifest &manifest) {
  (void)BackendAdapterRegistry::require(manifest.backend);
  if (manifest.transaction_marker_table != "benchbase.session_txn" ||
      manifest.control_table_allowlist !=
          std::vector<std::string>{"benchbase.checker_run_fence"} ||
      manifest.result_scope != "point_transaction_soww") {
    throw MonitorError(
        MonitorErrorCode::ConfigInvalid,
        "RunManifest backend/control/result contract is unsupported");
  }
  const auto &adapter = BenchmarkAdapterRegistry::require(manifest.benchmark);
  adapter.validateRunConfiguration(
      manifest.declared_isolation, manifest.application_table_allowlist,
      AdapterRuntimeCapabilities{.client_trace = false,
                                 .cdc_normalization = true,
                                 .bootstrap_scan = true});
  return adapter;
}

} // namespace

class RunController::Impl final {
public:
  Impl(RunManifest manifest, RunControllerConfig config)
      : manifest_(std::move(manifest)), config_(std::move(config)),
        backend_adapter_(BackendAdapterRegistry::require(manifest_.backend)),
        benchmark_adapter_(validatedBenchmarkAdapter(manifest_)),
        record_parser_(config_.record_parser_limits),
        client_parser_(manifest_.run_id, manifest_.expected_session_ids,
                       benchmark_adapter_, config_.client_parser_limits),
        joiner_(TransactionJoinerConfig{
            .run_id = manifest_.run_id,
            .expected_session_ids = manifest_.expected_session_ids,
            .application_tables = manifest_.application_table_allowlist,
            .benchmark_adapter = &benchmark_adapter_,
            .client_parser_limits = config_.client_parser_limits,
            .limits = config_.joiner_limits,
            .execute_consistency_check =
                config_.processing_mode == ProcessingMode::Audited,
        }) {
    validateConfig();
    for (const auto &session_id : manifest_.expected_session_ids) {
      SessionLedger ledger;
      ledger.digest = sessionMarkerDigestSeed(manifest_.run_id, session_id);
      sessions_.emplace(session_id, std::move(ledger));
    }
  }

  void setLastCdcStreamId(std::string stream_id) {
    requireStreamId(stream_id, "CDC");
    last_cdc_stream_id_ = std::move(stream_id);
  }

  void setLastClientStreamId(std::string stream_id) {
    requireStreamId(stream_id, "client");
    last_client_stream_id_ = std::move(stream_id);
  }

  void acceptClientPayload(const std::string_view payload) {
    if (terminal_) {
      return;
    }
    try {
      switch (record_parser_.kind(payload)) {
      case ProtocolRecordKind::RunManifest:
        (void)record_parser_.parseRunManifest(payload);
        invalidate(MonitorErrorCode::RunManifestDuplicate,
                   "RunManifest is duplicated after run admission");
        break;
      case ProtocolRecordKind::SchemaAttestation:
        try {
          acceptSchemaAttestation(record_parser_.parseSchemaAttestation(payload));
        } catch (const MonitorError &error) {
          throw MonitorError(MonitorErrorCode::SchemaAttestationMalformed,
                             error.what());
        }
        break;
      case ProtocolRecordKind::BootstrapManifest:
        acceptBootstrapManifest(record_parser_.parseBootstrapManifest(payload));
        break;
      case ProtocolRecordKind::ClientTxn:
        acceptClientTransaction(client_parser_.parse(payload));
        break;
      case ProtocolRecordKind::SessionDone:
        acceptSessionDone(record_parser_.parseSessionDone(payload));
        break;
      case ProtocolRecordKind::RunFailed:
        acceptRunFailed(record_parser_.parseRunFailed(payload));
        break;
      }
    } catch (const MonitorError &error) {
      invalidate(error.code(), error.what());
    } catch (const std::bad_alloc &) {
      invalidate(MonitorErrorCode::ResourceExhausted,
                 "allocation failed while accepting client evidence");
    } catch (const std::length_error &) {
      invalidate(MonitorErrorCode::ResourceExhausted,
                 "container limit failed while accepting client evidence");
    } catch (const std::exception &error) {
      invalidate(MonitorErrorCode::InternalError,
                 std::string("unexpected client evidence failure: ") +
                     error.what());
    }
    synchronize();
  }

  void acceptCdcTransaction(CdcTransaction transaction) {
    if (terminal_) {
      return;
    }
    if (transaction.cdc_seq >= max_json_integer) {
      invalidate(MonitorErrorCode::ResourceExhausted,
                 "assembled CDC counter exceeds the JSON-safe range");
      return;
    }
    assembled_cdc_transactions_ = transaction.cdc_seq + 1U;
    if (transaction.kind == CdcTransactionKind::RunControl &&
        transaction.fence_phase == RunFencePhase::EndFence) {
      end_fence_assembled_ = true;
    }
    if (!admission_stopped_ && !joiner_.failed()) {
      try {
        joiner_.acceptCdcTransaction(std::move(transaction));
      } catch (const MonitorError &error) {
        invalidate(error.code(), error.what());
      } catch (const std::bad_alloc &) {
        invalidate(MonitorErrorCode::ResourceExhausted,
                   "allocation failed while accepting CDC evidence");
      } catch (const std::exception &error) {
        invalidate(MonitorErrorCode::InternalError,
                   std::string("unexpected CDC admission failure: ") +
                       error.what());
      }
    }
    synchronize();
  }

  void noteExternalFailure(const DiagnosticCategory category, std::string code,
                           std::string message) {
    if (terminal_) {
      return;
    }
    invalid_ = true;
    admission_stopped_ = true;
    addDiagnostic(category, std::move(code), std::move(message));
    enterDrainingIfPossible();
  }

  void noteExternalFailure(const std::string_view category, std::string code,
                           std::string message) {
    DiagnosticCategory parsed = DiagnosticCategory::Operational;
    if (category == "CHECKER") {
      parsed = DiagnosticCategory::Checker;
    } else if (category == "PROTOCOL") {
      parsed = DiagnosticCategory::Protocol;
    } else if (category != "OPERATIONAL") {
      code = "INTERNAL_ERROR";
      message = "invalid diagnostic category: " + std::string(category);
    }
    noteExternalFailure(parsed, std::move(code), std::move(message));
  }

  void onInputIdle() {
    if (!terminal_ && endFenceObserved() && final_schema_attestation_.has_value()) {
      auditAndFinalize();
    }
  }

  void forceTimeoutFinal() {
    if (terminal_) {
      return;
    }
    if (!endFenceObserved()) {
      invalidate(MonitorErrorCode::FenceMissing,
                 "monitor timed out before END_FENCE");
    }
    if (!startup_schema_attestation_.has_value() ||
        !final_schema_attestation_.has_value()) {
      invalidate(MonitorErrorCode::SchemaAttestationMissing,
                 "monitor timed out before both schema attestations arrived");
    }
    addMissingClosureDiagnostics();
    invalid_ = true;
    auditAndFinalize();
  }

  void forceInvalidFinal(const MonitorErrorCode code, std::string message) {
    forceInvalidFinal(std::string(blackbox::toString(code)),
                      std::move(message));
  }

  void forceInvalidFinal(std::string code, std::string message) {
    if (terminal_) {
      return;
    }
    invalid_ = true;
    addDiagnostic(DiagnosticCategory::Protocol, std::move(code),
                  std::move(message));
    auditAndFinalize();
  }

  [[nodiscard]] bool terminal() const noexcept { return terminal_; }

  [[nodiscard]] bool endFenceObserved() const noexcept {
    return end_fence_assembled_ || joiner_.metrics().end_fence_observed;
  }

  [[nodiscard]] bool endFenceProcessed() const noexcept {
    return joiner_.metrics().end_fence_processed;
  }

  [[nodiscard]] ProcessingMode processingMode() const noexcept {
    return config_.processing_mode;
  }

  [[nodiscard]] std::optional<FinalOutcome> finalOutcome() const noexcept {
    return final_outcome_;
  }

  [[nodiscard]] const std::optional<ReconciliationSummary> &
  finalReconciliation() const noexcept {
    return final_reconciliation_;
  }

  [[nodiscard]] const std::vector<RunDiagnostic> &diagnostics() const noexcept {
    return diagnostics_;
  }

  [[nodiscard]] TransactionJoinerMetrics joinerMetrics() const {
    return joiner_.metrics();
  }

  [[nodiscard]] std::vector<std::string> takePendingStatuses() {
    std::vector<std::string> result;
    result.swap(pending_statuses_);
    return result;
  }

  [[nodiscard]] std::string finalReport() const {
    if (config_.processing_mode == ProcessingMode::JoinOnly) {
      const bool evidence_reconciled =
          final_reconciliation_.has_value() &&
          final_reconciliation_->evidence_reconciled;
      const std::string_view completion_status =
          !terminal_ ? "running"
                     : (evidence_reconciled ? "completed" : "failed");
      const auto metrics = joiner_.metrics();
      std::ostringstream result;
      result << "run_id=" << manifest_.run_id << " processing_mode="
             << blackbox::toString(config_.processing_mode)
             << " completion_status=" << completion_status
             << " evidence_reconciled="
             << (evidence_reconciled ? "true" : "false")
             << " accepted_client_transactions="
             << metrics.accepted_client_transactions
             << " assembled_cdc_transactions=" << assembled_cdc_transactions_
             << " joined_workload_transactions="
             << metrics.joined_workload_transactions
             << " released_bootstrap_transactions="
             << metrics.released_bootstrap_transactions
             << " released_phase_anchor_transactions="
             << metrics.released_phase_anchor_transactions
             << " released_workload_transactions="
             << metrics.released_workload_transactions
             << " diagnostics=" << diagnostics_.size();
      return result.str();
    }
    if (!terminal_ || !final_outcome_.has_value() ||
        !final_reconciliation_.has_value()) {
      return "run_id=" + manifest_.run_id + " outcome=RUNNING";
    }
    const auto &reconciliation = *final_reconciliation_;
    std::ostringstream result;
    result << "run_id=" << manifest_.run_id
           << " outcome=" << blackbox::toString(*final_outcome_)
           << " evidence_reconciled="
           << (reconciliation.evidence_reconciled ? "true" : "false")
           << " expected_sessions=" << reconciliation.expected_sessions
           << " completed_sessions=" << reconciliation.completed_sessions
           << " accepted_client_transactions="
           << reconciliation.accepted_client_transactions
           << " assembled_cdc_transactions=" << assembled_cdc_transactions_
           << " joined_workload_transactions="
           << reconciliation.joined_workload_transactions
           << " checker_consumed_transactions="
           << reconciliation.checker_consumed_transactions
           << " unmatched_client_records="
           << reconciliation.unmatched_client_records
           << " unmatched_workload_cdc_slots="
           << reconciliation.unmatched_workload_cdc_slots
           << " diagnostics=" << diagnostics_.size();
    return result.str();
  }

private:
  struct SessionLedger {
    SessionMarkerDigest digest{};
    std::uint64_t accepted_transactions = 0;
    std::uint64_t last_session_seq = 0;
    std::uint64_t last_txn_id = 0;
    bool done_observed = false;
    bool done_valid = false;
    std::optional<SessionDone> done;
  };

  void validateConfig() const {
    if (manifest_.expected_session_ids.empty() ||
        manifest_.application_table_allowlist.empty() ||
        config_.limits.max_diagnostics == 0U ||
        config_.limits.max_diagnostics > 64U ||
        config_.limits.max_pending_status_records < 8U ||
        config_.limits.max_diagnostic_message_bytes == 0U ||
        config_.limits.max_diagnostic_message_bytes > 4096U ||
        (config_.processing_mode != ProcessingMode::Audited &&
         config_.processing_mode != ProcessingMode::JoinOnly)) {
      throw std::invalid_argument("run controller configuration is invalid");
    }
    const std::set<std::string> sessions(manifest_.expected_session_ids.begin(),
                                         manifest_.expected_session_ids.end());
    if (sessions.size() != manifest_.expected_session_ids.size()) {
      throw std::invalid_argument("RunManifest session roster is duplicated");
    }
  }

  static void requireStreamId(const std::string_view stream_id,
                              const std::string_view label) {
    const auto separator = stream_id.find('-');
    if (separator == std::string_view::npos || separator == 0U ||
        separator + 1U == stream_id.size()) {
      throw std::invalid_argument(std::string(label) + " stream ID is invalid");
    }
    const auto component = [](const std::string_view value) {
      return !value.empty() && (value.size() == 1U || value.front() != '0') &&
             value.find_first_not_of("0123456789") == std::string_view::npos;
    };
    if (!component(stream_id.substr(0U, separator)) ||
        !component(stream_id.substr(separator + 1U))) {
      throw std::invalid_argument(std::string(label) + " stream ID is invalid");
    }
  }

  void acceptSchemaAttestation(SchemaAttestation attestation) {
    if (attestation.run_id != manifest_.run_id ||
        attestation.backend != manifest_.backend ||
        attestation.backend_source_id != manifest_.backend_source_id) {
      invalidate(MonitorErrorCode::SchemaAttestationMismatch,
                 "SchemaAttestation identity differs from RunManifest");
      return;
    }
    try {
      backend_adapter_.validateSchemaAttestation(attestation,
                                                 benchmark_adapter_);
    } catch (const MonitorError &error) {
      invalidate(error.code(), error.what());
      return;
    }
    if (attestation.phase == SchemaAttestationPhase::Startup) {
      if (startup_schema_attestation_.has_value()) {
        invalidate(MonitorErrorCode::SchemaAttestationDuplicate,
                   "STARTUP SchemaAttestation is duplicated");
        return;
      }
      if (lifecycle_state_ != LifecycleState::Starting) {
        invalidate(MonitorErrorCode::SchemaAttestationMismatch,
                   "STARTUP SchemaAttestation arrived after run admission");
        return;
      }
      startup_schema_attestation_ = std::move(attestation);
      return;
    }
    if (final_schema_attestation_.has_value()) {
      invalidate(MonitorErrorCode::SchemaAttestationDuplicate,
                 "FINAL SchemaAttestation is duplicated");
      return;
    }
    if (!startup_schema_attestation_.has_value() || !endFenceObserved()) {
      invalidate(MonitorErrorCode::SchemaAttestationMismatch,
                 "FINAL SchemaAttestation arrived before STARTUP/END_FENCE");
      return;
    }
    const auto &startup = *startup_schema_attestation_;
    if (attestation.tables != startup.tables ||
        attestation.backend_facts != startup.backend_facts ||
        attestation.canonical_attestation_digest !=
            startup.canonical_attestation_digest) {
      invalidate(MonitorErrorCode::SchemaAttestationDrift,
                 "STARTUP and FINAL schema/capture attestations differ");
      return;
    }
    final_schema_attestation_ = std::move(attestation);
  }

  void acceptBootstrapManifest(BootstrapManifest manifest) {
    if (manifest.run_id != manifest_.run_id) {
      invalidate(MonitorErrorCode::RunIdMismatch,
                 "BootstrapManifest belongs to another run");
      return;
    }
    if (bootstrap_manifest_.has_value()) {
      invalidate(MonitorErrorCode::BootstrapStateMismatch,
                 "BootstrapManifest is duplicated");
      return;
    }
    std::vector<std::string> tables;
    tables.reserve(manifest.table_row_counts.size());
    for (const auto &entry : manifest.table_row_counts) {
      tables.push_back(entry.table);
    }
    if (tables != sortedStrings(manifest_.application_table_allowlist)) {
      invalidate(MonitorErrorCode::BootstrapStateMismatch,
                 "BootstrapManifest table roster differs from RunManifest");
      return;
    }
    bootstrap_manifest_ = std::move(manifest);
    submitBootstrapIfReady();
  }

  void acceptClientTransaction(ClientTxn transaction) {
    if (admission_stopped_) {
      return;
    }
    auto found = sessions_.find(transaction.session_id);
    if (found == sessions_.end()) {
      invalidate(MonitorErrorCode::SessionUnknown,
                 "ClientTxn names an unknown session");
      return;
    }
    if (found->second.done_observed) {
      invalidate(MonitorErrorCode::ClientRecordAfterDone,
                 "ClientTxn follows SessionDone for its session");
      return;
    }
    const auto session_seq = transaction.session_seq;
    const auto txn_id = transaction.correlation_id.txn_id;
    try {
      joiner_.acceptClientTransaction(std::move(transaction));
    } catch (const MonitorError &error) {
      invalidate(error.code(), error.what());
      return;
    }
    auto &ledger = found->second;
    ledger.digest = sessionMarkerDigestStep(
        ledger.digest,
        SessionMarkerEntry{.session_seq = session_seq, .txn_id = txn_id});
    ++ledger.accepted_transactions;
    ledger.last_session_seq = session_seq;
    ledger.last_txn_id = txn_id;
  }

  void acceptSessionDone(SessionDone done) {
    if (done.run_id != manifest_.run_id) {
      invalidate(MonitorErrorCode::RunIdMismatch,
                 "SessionDone belongs to another run");
      return;
    }
    auto found = sessions_.find(done.session_id);
    if (found == sessions_.end()) {
      invalidate(MonitorErrorCode::SessionUnknown,
                 "SessionDone names an unknown session");
      return;
    }
    auto &ledger = found->second;
    if (ledger.done_observed) {
      invalidate(MonitorErrorCode::SessionDoneMismatch,
                 "SessionDone is duplicated for one session");
      return;
    }
    ledger.done_observed = true;
    ++completed_sessions_;
    ledger.done = done;
    const bool logical_progress_valid =
        ledger.last_session_seq <= done.logical_transactions_started;
    const bool physical_progress_valid =
        ledger.last_txn_id <= done.physical_attempts_started;
    bool final_attempt_history_feasible = false;
    if (logical_progress_valid && physical_progress_valid) {
      const auto remaining_logical_transactions =
          done.logical_transactions_started - ledger.last_session_seq;
      const auto remaining_physical_attempts =
          done.physical_attempts_started - ledger.last_txn_id;
      final_attempt_history_feasible =
          remaining_logical_transactions == 0U
              ? remaining_physical_attempts == 0U
              : remaining_physical_attempts >= remaining_logical_transactions;
    }
    bool valid = true;
    valid =
        valid && done.published_transactions == ledger.accepted_transactions;
    valid = valid && logical_progress_valid;
    valid = valid && physical_progress_valid;
    valid = valid && final_attempt_history_feasible;
    valid = valid && sessionMarkerDigestHex(ledger.digest) ==
                         done.rolling_correlation_digest;
    ledger.done_valid = valid;
    if (!valid) {
      invalidate(MonitorErrorCode::SessionDoneMismatch,
                 "SessionDone counters or rolling marker digest do not match "
                 "accepted ClientTxn evidence");
    }
  }

  void acceptRunFailed(const RunFailed &failed) {
    if (failed.run_id != manifest_.run_id) {
      invalidate(MonitorErrorCode::RunIdMismatch,
                 "RunFailed belongs to another run");
      return;
    }
    if (failed.session_id.has_value() &&
        !sessions_.contains(*failed.session_id)) {
      invalidate(MonitorErrorCode::SessionUnknown,
                 "RunFailed names an unknown session");
      return;
    }
    if (failed.reason_code == "COMMIT_OUTCOME_AMBIGUOUS") {
      if (ambiguous_attempts_ == max_json_integer) {
        invalidate(MonitorErrorCode::ResourceExhausted,
                   "ambiguous attempt counter exceeds the JSON-safe range");
        return;
      }
      ++ambiguous_attempts_;
    }
    invalid_ = true;
    admission_stopped_ = true;
    addDiagnostic(DiagnosticCategory::Operational, failed.reason_code,
                  failed.diagnostic);
  }

  void submitBootstrapIfReady() {
    if (bootstrap_submitted_ || !bootstrap_manifest_.has_value()) {
      return;
    }
    const auto prefix = joiner_.pendingBootstrapPrefix();
    if (!prefix.has_value()) {
      return;
    }
    std::vector<BootstrapTableCount> table_counts;
    table_counts.reserve(bootstrap_manifest_->table_row_counts.size());
    for (const auto &entry : bootstrap_manifest_->table_row_counts) {
      table_counts.push_back(BootstrapTableCount{.table = entry.table,
                                                 .row_count = entry.row_count});
    }
    try {
      joiner_.validateBootstrapState(BootstrapStateProof{
          .prefix = *prefix,
          .table_row_counts = std::move(table_counts),
          .canonical_state_digest = bootstrap_manifest_->canonical_state_digest,
      });
      bootstrap_submitted_ = true;
    } catch (const MonitorError &error) {
      invalidate(error.code(), error.what());
    }
  }

  void synchronize() {
    submitBootstrapIfReady();
    recordFirstViolation();
    const auto lifecycle = joiner_.lifecycle();
    if ((lifecycle == JoinLifecycle::Bootstrap ||
         lifecycle == JoinLifecycle::Workload) &&
        lifecycle_state_ == LifecycleState::Starting &&
        startup_schema_attestation_.has_value()) {
      queueStatus(LifecycleState::StartFenceSeen);
      queueStatus(LifecycleState::Bootstrap);
    }
    if (lifecycle == JoinLifecycle::Workload &&
        (lifecycle_state_ == LifecycleState::Bootstrap ||
         lifecycle_state_ == LifecycleState::StartFenceSeen)) {
      queueStatus(LifecycleState::LoadFinishedSeen);
      queueStatus(LifecycleState::Running);
    }
    enterDrainingIfPossible();
    const auto observation = joiner_.checkerObservation();
    if (observation != last_published_observation_ &&
        (lifecycle_state_ == LifecycleState::Running ||
         lifecycle_state_ == LifecycleState::Draining)) {
      queueStatus(lifecycle_state_, true);
    }
  }

  void enterDrainingIfPossible() {
    if ((endFenceObserved() || completed_sessions_ == sessions_.size() ||
         invalid_) &&
        lifecycle_state_ != LifecycleState::Starting &&
        lifecycle_state_ != LifecycleState::StartFenceSeen &&
        lifecycle_state_ != LifecycleState::Bootstrap &&
        lifecycle_state_ != LifecycleState::LoadFinishedSeen &&
        lifecycle_state_ != LifecycleState::Draining &&
        lifecycle_state_ != LifecycleState::Reconciled &&
        lifecycle_state_ != LifecycleState::Final) {
      queueStatus(LifecycleState::Draining);
    }
  }

  [[nodiscard]] std::size_t auditedCompletedSessionCount() const {
    return static_cast<std::size_t>(std::count_if(
        sessions_.begin(), sessions_.end(),
        [](const auto &entry) { return entry.second.done_observed; }));
  }

  [[nodiscard]] std::uint64_t publishedTransactionTotal() const {
    std::uint64_t result = 0;
    for (const auto &[unused, ledger] : sessions_) {
      (void)unused;
      if (!ledger.done.has_value() ||
          ledger.done->published_transactions > max_json_integer - result) {
        continue;
      }
      result += ledger.done->published_transactions;
    }
    return result;
  }

  void addMissingClosureDiagnostics() {
    if (!startup_schema_attestation_.has_value() ||
        !final_schema_attestation_.has_value()) {
      invalidate(MonitorErrorCode::SchemaAttestationMissing,
                 "STARTUP or FINAL SchemaAttestation is missing at closure");
    }
    if (!bootstrap_manifest_.has_value() ||
        !joiner_.metrics().bootstrap_state_validated) {
      invalidate(MonitorErrorCode::BootstrapManifestMissing,
                 "validated BootstrapManifest is missing at closure");
    }
    for (const auto &[session_id, ledger] : sessions_) {
      if (!ledger.done_observed) {
        invalidate(MonitorErrorCode::SessionDoneMissing,
                   "SessionDone is missing for " + session_id);
      }
    }
  }

  void auditAndFinalize() {
    if (terminal_) {
      return;
    }
    recordFirstViolation();
    enterDrainingIfPossible();
    addMissingClosureDiagnostics();

    auto metrics = joiner_.metrics();
    const auto joiner_audit = joiner_.auditReconciliationState();
    const auto audited_completed_sessions = auditedCompletedSessionCount();
    if (!joiner_audit.incremental_counters_match ||
        joiner_audit.pending_client_records != metrics.pending_client_records ||
        joiner_audit.pending_cdc_slots != metrics.pending_cdc_slots ||
        audited_completed_sessions != completed_sessions_) {
      invalidate(MonitorErrorCode::ReconciliationMismatch,
                 "incremental reconciliation counters disagree with the "
                 "terminal evidence audit");
    }
    if (joiner_audit.unmatched_workload_cdc_slots != 0U ||
        joiner_.headBlockReason() == HeadBlockReason::MissingClient) {
      invalidate(MonitorErrorCode::ClientHalfMissing,
                 "one or more workload CDC slots lack client evidence");
    }
    if (joiner_audit.unmatched_client_records != 0U) {
      invalidate(MonitorErrorCode::CdcHalfMissing,
                 "one or more client transactions lack workload CDC evidence");
    }

    if (joiner_.headBlockReason() == HeadBlockReason::EndFenceDeferred &&
        !metrics.end_fence_processed) {
      try {
        joiner_.processEndFence();
      } catch (const MonitorError &error) {
        invalidate(error.code(), error.what());
      }
    }
    metrics = joiner_.metrics();

    const auto expected_sessions = counter(sessions_.size());
    const auto completed_sessions = counter(audited_completed_sessions);
    const bool schema_attestations_match =
        startup_schema_attestation_.has_value() &&
        final_schema_attestation_.has_value() &&
        startup_schema_attestation_->tables == final_schema_attestation_->tables &&
        startup_schema_attestation_->backend_facts ==
            final_schema_attestation_->backend_facts &&
        startup_schema_attestation_->canonical_attestation_digest ==
            final_schema_attestation_->canonical_attestation_digest;
    bool exact = false;
    if (config_.processing_mode == ProcessingMode::Audited) {
      const auto expected_checker_transactions =
          metrics.joined_workload_transactions +
          metrics.checker.bootstrap_transactions +
          metrics.checker.phase_anchor_transactions;
      exact =
          schema_attestations_match && endFenceObserved() &&
          metrics.end_fence_processed &&
          metrics.bootstrap_state_validated &&
          completed_sessions == expected_sessions &&
          publishedTransactionTotal() == metrics.accepted_client_transactions &&
          metrics.accepted_client_transactions ==
              metrics.accepted_workload_cdc_transactions &&
          metrics.accepted_client_transactions ==
              metrics.joined_workload_transactions &&
          metrics.accepted_control_cdc_transactions == 3U &&
          metrics.accepted_bootstrap_cdc_transactions ==
              metrics.checker.bootstrap_transactions &&
          metrics.checker.phase_anchor_transactions == expected_sessions &&
          metrics.checker.consumed_transactions ==
              expected_checker_transactions &&
          joiner_audit.incremental_counters_match &&
          joiner_audit.unmatched_client_records == 0U &&
          joiner_audit.unmatched_workload_cdc_slots == 0U &&
          audited_completed_sessions == completed_sessions_ &&
          ambiguous_attempts_ == 0U && metrics.pending_cdc_slots == 0U &&
          assembled_cdc_transactions_ == metrics.accepted_cdc_transactions;
    } else {
      exact =
          schema_attestations_match && endFenceObserved() &&
          metrics.end_fence_processed &&
          metrics.bootstrap_state_validated &&
          completed_sessions == expected_sessions &&
          publishedTransactionTotal() == metrics.accepted_client_transactions &&
          metrics.accepted_client_transactions ==
              metrics.accepted_workload_cdc_transactions &&
          metrics.accepted_client_transactions ==
              metrics.joined_workload_transactions &&
          metrics.accepted_client_transactions ==
              metrics.released_workload_transactions &&
          metrics.accepted_control_cdc_transactions == 3U &&
          metrics.accepted_bootstrap_cdc_transactions ==
              metrics.released_bootstrap_transactions &&
          metrics.released_phase_anchor_transactions == expected_sessions &&
          metrics.released_bootstrap_write_events <=
              std::numeric_limits<std::uint64_t>::max() -
                  metrics.joined_ordered_events &&
          metrics.released_ordered_events ==
              metrics.released_bootstrap_write_events +
                  metrics.joined_ordered_events &&
          metrics.checker.consumed_transactions == 0U &&
          metrics.checker.consume_total_nanoseconds == 0U &&
          metrics.checker.consume_max_nanoseconds == 0U &&
          metrics.checker.mapped_identity_records == 0U &&
          metrics.checker.mapped_identity_bytes == 0U &&
          metrics.checker.engine.retained_state_bytes == 0U &&
          metrics.checker.engine.peak_retained_state_bytes == 0U &&
          joiner_.checkerObservation() == CheckerObservation::NotStarted &&
          !joiner_.firstViolation().has_value() &&
          joiner_audit.incremental_counters_match &&
          joiner_audit.unmatched_client_records == 0U &&
          joiner_audit.unmatched_workload_cdc_slots == 0U &&
          audited_completed_sessions == completed_sessions_ &&
          ambiguous_attempts_ == 0U && metrics.pending_cdc_slots == 0U &&
          assembled_cdc_transactions_ == metrics.accepted_cdc_transactions;
    }
    if (!exact) {
      invalidate(MonitorErrorCode::ReconciliationMismatch,
                 "final cross-component reconciliation equalities do not hold");
    }

    recordFirstViolation();

    final_reconciliation_ = ReconciliationSummary{
        .evidence_reconciled = exact && !invalid_,
        .startup_schema_attested = startup_schema_attestation_.has_value(),
        .final_schema_attested = final_schema_attestation_.has_value(),
        .schema_attestation_digest =
            startup_schema_attestation_.has_value()
                ? startup_schema_attestation_->canonical_attestation_digest
                : std::string{},
        .bootstrap_manifest_validated = metrics.bootstrap_state_validated,
        .end_fence_seen = endFenceObserved(),
        .expected_sessions = expected_sessions,
        .completed_sessions = completed_sessions,
        .accepted_client_transactions = metrics.accepted_client_transactions,
        .joined_workload_transactions = metrics.joined_workload_transactions,
        .bootstrap_cdc_transactions =
            metrics.accepted_bootstrap_cdc_transactions,
        .bootstrap_checker_transactions =
            metrics.checker.bootstrap_transactions,
        .phase_anchor_transactions = metrics.checker.phase_anchor_transactions,
        .checker_consumed_transactions = metrics.checker.consumed_transactions,
        .unmatched_client_records =
            counter(joiner_audit.unmatched_client_records),
        .unmatched_workload_cdc_slots =
            counter(joiner_audit.unmatched_workload_cdc_slots),
        .ambiguous_attempts = ambiguous_attempts_,
    };

    if (config_.processing_mode == ProcessingMode::JoinOnly) {
      lifecycle_state_ = LifecycleState::Final;
      terminal_ = true;
      return;
    }
    if (!invalid_) {
      queueStatus(LifecycleState::Reconciled);
      final_outcome_ =
          joiner_.checkerObservation() == CheckerObservation::ViolationObserved
              ? FinalOutcome::Violation
              : FinalOutcome::Pass;
    } else {
      final_outcome_ = FinalOutcome::Invalid;
    }
    lifecycle_state_ = LifecycleState::Final;
    terminal_ = true;
    queueStatus(LifecycleState::Final, true);
  }

  void invalidate(const MonitorErrorCode code, std::string message) {
    invalid_ = true;
    admission_stopped_ = true;
    addDiagnostic(DiagnosticCategory::Protocol,
                  std::string(blackbox::toString(code)), std::move(message));
  }

  void addDiagnostic(const DiagnosticCategory category, std::string code,
                     std::string message) {
    if (!isErrorCode(code)) {
      code = "INTERNAL_ERROR";
    }
    message = boundedMessage(std::move(message),
                             config_.limits.max_diagnostic_message_bytes);
    const RunDiagnostic candidate{.category = category,
                                  .code = std::move(code),
                                  .message = std::move(message)};
    if (std::any_of(diagnostics_.begin(), diagnostics_.end(),
                    [&candidate](const RunDiagnostic &existing) {
                      return existing.code == candidate.code &&
                             existing.message == candidate.message;
                    })) {
      return;
    }
    if (diagnostics_.size() >= config_.limits.max_diagnostics) {
      invalid_ = true;
      return;
    }
    diagnostics_.push_back(candidate);
  }

  void recordFirstViolation() {
    if (first_violation_recorded_ || !joiner_.firstViolation().has_value()) {
      return;
    }
    const auto &violation = *joiner_.firstViolation();
    RunDiagnostic diagnostic{
        .category = DiagnosticCategory::Checker,
        .code = "VIOLATION_OBSERVED",
        .message = boundedMessage(
            "checker violation at commit_seq=" +
                std::to_string(violation.checker_violation.commit_seq) +
                " session_id=" + violation.session_id +
                " logical_txn_id=" + violation.logical_txn_id +
                " attempt_id=" + violation.attempt_id,
            config_.limits.max_diagnostic_message_bytes),
    };
    // The first attributed violation owns a priority slot. If an earlier
    // operational diagnostic filled the bounded ledger, evict one ordinary
    // entry rather than losing the checker evidence.
    if (diagnostics_.size() >= config_.limits.max_diagnostics) {
      const auto replaceable =
          std::find_if(diagnostics_.rbegin(), diagnostics_.rend(),
                       [](const RunDiagnostic &existing) {
                         return existing.code != "VIOLATION_OBSERVED";
                       });
      if (replaceable != diagnostics_.rend()) {
        diagnostics_.erase(std::next(replaceable).base());
      }
    }
    if (diagnostics_.size() < config_.limits.max_diagnostics) {
      diagnostics_.push_back(std::move(diagnostic));
      first_violation_recorded_ = true;
    }
  }

  [[nodiscard]] std::string buildStatus(const LifecycleState state) const {
    const auto metrics = joiner_.metrics();
    Json diagnostic_array = Json::array();
    for (const auto &diagnostic : diagnostics_) {
      diagnostic_array.push_back(
          Json{{"category", blackbox::toString(diagnostic.category)},
               {"code", diagnostic.code},
               {"message", diagnostic.message}});
    }
    Json status{
        {"kind", "status"},
        {"run_id", manifest_.run_id},
        {"lifecycle_state", toString(state)},
        {"outcome", state == LifecycleState::Final
                        ? std::string(blackbox::toString(*final_outcome_))
                        : std::string("RUNNING")},
        {"checker_observation",
         blackbox::toString(joiner_.checkerObservation())},
        {"last_cdc_stream_id", last_cdc_stream_id_},
        {"last_client_stream_id", last_client_stream_id_},
        {"assembled_cdc_transactions", assembled_cdc_transactions_},
        {"joined_transactions", metrics.joined_workload_transactions},
        {"checked_transactions", metrics.checker.consumed_transactions},
        {"pending_cdc_slots", metrics.pending_cdc_slots},
        {"unmatched_client_records", metrics.unmatched_client_records},
        {"diagnostics", std::move(diagnostic_array)},
    };
    if (state == LifecycleState::Final) {
      const auto &reconciliation = *final_reconciliation_;
      status["reconciliation"] = Json{
          {"evidence_reconciled", reconciliation.evidence_reconciled},
          {"startup_schema_attested",
           reconciliation.startup_schema_attested},
          {"final_schema_attested", reconciliation.final_schema_attested},
          {"schema_attestation_digest",
           reconciliation.schema_attestation_digest},
          {"bootstrap_manifest_validated",
           reconciliation.bootstrap_manifest_validated},
          {"end_fence_seen", reconciliation.end_fence_seen},
          {"expected_sessions", reconciliation.expected_sessions},
          {"completed_sessions", reconciliation.completed_sessions},
          {"accepted_client_transactions",
           reconciliation.accepted_client_transactions},
          {"joined_workload_transactions",
           reconciliation.joined_workload_transactions},
          {"bootstrap_cdc_transactions",
           reconciliation.bootstrap_cdc_transactions},
          {"bootstrap_checker_transactions",
           reconciliation.bootstrap_checker_transactions},
          {"phase_anchor_transactions",
           reconciliation.phase_anchor_transactions},
          {"checker_consumed_transactions",
           reconciliation.checker_consumed_transactions},
          {"unmatched_client_records", reconciliation.unmatched_client_records},
          {"unmatched_workload_cdc_slots",
           reconciliation.unmatched_workload_cdc_slots},
          {"ambiguous_attempts", reconciliation.ambiguous_attempts},
      };
    }
    return status.dump(-1, ' ', false, Json::error_handler_t::replace);
  }

  void queueStatus(const LifecycleState state, const bool allow_same = false) {
    if (!allow_same && state == lifecycle_state_ &&
        !pending_statuses_.empty()) {
      return;
    }
    if (pending_statuses_.size() >= config_.limits.max_pending_status_records) {
      throw MonitorError(MonitorErrorCode::ResourceExhausted,
                         "pending monitor status queue exceeds its limit");
    }
    lifecycle_state_ = state;
    pending_statuses_.push_back(buildStatus(state));
    last_published_observation_ = joiner_.checkerObservation();
  }

  RunManifest manifest_;
  RunControllerConfig config_;
  const BackendAdapter &backend_adapter_;
  const BenchmarkAdapter &benchmark_adapter_;
  ProtocolRecordParser record_parser_;
  ClientTxnParser client_parser_;
  TransactionJoiner joiner_;
  std::unordered_map<std::string, SessionLedger> sessions_;
  std::optional<BootstrapManifest> bootstrap_manifest_;
  std::optional<SchemaAttestation> startup_schema_attestation_;
  std::optional<SchemaAttestation> final_schema_attestation_;
  std::vector<RunDiagnostic> diagnostics_;
  std::vector<std::string> pending_statuses_;
  std::optional<ReconciliationSummary> final_reconciliation_;
  std::optional<FinalOutcome> final_outcome_;
  std::string last_cdc_stream_id_ = "0-0";
  std::string last_client_stream_id_ = "0-0";
  LifecycleState lifecycle_state_ = LifecycleState::Starting;
  CheckerObservation last_published_observation_ =
      CheckerObservation::NotStarted;
  std::uint64_t assembled_cdc_transactions_ = 0;
  std::uint64_t ambiguous_attempts_ = 0;
  std::size_t completed_sessions_ = 0;
  bool bootstrap_submitted_ = false;
  bool first_violation_recorded_ = false;
  bool end_fence_assembled_ = false;
  bool invalid_ = false;
  bool admission_stopped_ = false;
  bool terminal_ = false;
};

RunController::RunController(RunManifest manifest, RunControllerConfig config)
    : impl_(std::make_unique<Impl>(std::move(manifest), std::move(config))) {}

RunController::~RunController() = default;
RunController::RunController(RunController &&) noexcept = default;
RunController &RunController::operator=(RunController &&) noexcept = default;

void RunController::setLastCdcStreamId(std::string stream_id) {
  impl_->setLastCdcStreamId(std::move(stream_id));
}

void RunController::setLastClientStreamId(std::string stream_id) {
  impl_->setLastClientStreamId(std::move(stream_id));
}

void RunController::acceptClientPayload(const std::string_view payload) {
  impl_->acceptClientPayload(payload);
}

void RunController::acceptCdcTransaction(CdcTransaction transaction) {
  impl_->acceptCdcTransaction(std::move(transaction));
}

void RunController::noteExternalFailure(const DiagnosticCategory category,
                                        std::string code, std::string message) {
  impl_->noteExternalFailure(category, std::move(code), std::move(message));
}

void RunController::noteExternalFailure(const std::string_view category,
                                        std::string code, std::string message) {
  impl_->noteExternalFailure(category, std::move(code), std::move(message));
}

void RunController::onInputIdle() { impl_->onInputIdle(); }

void RunController::forceTimeoutFinal() { impl_->forceTimeoutFinal(); }

void RunController::forceInvalidFinal(const MonitorErrorCode code,
                                      std::string message) {
  impl_->forceInvalidFinal(code, std::move(message));
}

void RunController::forceInvalidFinal(std::string code, std::string message) {
  impl_->forceInvalidFinal(std::move(code), std::move(message));
}

bool RunController::terminal() const noexcept { return impl_->terminal(); }

bool RunController::endFenceObserved() const noexcept {
  return impl_->endFenceObserved();
}

bool RunController::endFenceProcessed() const noexcept {
  return impl_->endFenceProcessed();
}

ProcessingMode RunController::processingMode() const noexcept {
  return impl_->processingMode();
}

std::optional<FinalOutcome> RunController::finalOutcome() const noexcept {
  return impl_->finalOutcome();
}

const std::optional<ReconciliationSummary> &
RunController::finalReconciliation() const noexcept {
  return impl_->finalReconciliation();
}

const std::vector<RunDiagnostic> &RunController::diagnostics() const noexcept {
  return impl_->diagnostics();
}

TransactionJoinerMetrics RunController::joinerMetrics() const {
  return impl_->joinerMetrics();
}

std::vector<std::string> RunController::takePendingStatuses() {
  return impl_->takePendingStatuses();
}

std::string RunController::finalReport() const { return impl_->finalReport(); }

std::string_view toString(const DiagnosticCategory category) noexcept {
  switch (category) {
  case DiagnosticCategory::Checker:
    return "CHECKER";
  case DiagnosticCategory::Protocol:
    return "PROTOCOL";
  case DiagnosticCategory::Operational:
    return "OPERATIONAL";
  }
  std::abort();
}

std::string_view toString(const FinalOutcome outcome) noexcept {
  switch (outcome) {
  case FinalOutcome::Pass:
    return "PASS";
  case FinalOutcome::Violation:
    return "VIOLATION";
  case FinalOutcome::Invalid:
    return "INVALID";
  }
  std::abort();
}

std::string_view toString(const ProcessingMode mode) noexcept {
  switch (mode) {
  case ProcessingMode::Audited:
    return "audited";
  case ProcessingMode::JoinOnly:
    return "join-only";
  }
  std::abort();
}

} // namespace blackbox
