#include "blackbox/run_controller.h"

#include "blackbox/canonical_ycsb_codec.h"
#include "blackbox/client_txn.h"
#include "test_support.h"

#include <nlohmann/json.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace {

using blackbox::BootstrapManifest;
using blackbox::CanonicalBytes;
using blackbox::CanonicalYcsbCodec;
using blackbox::CdcTransaction;
using blackbox::CdcTransactionKind;
using blackbox::ClientRead;
using blackbox::ClientReadSource;
using blackbox::ClientValueState;
using blackbox::FinalOutcome;
using blackbox::NormalizedWrite;
using blackbox::RunController;
using blackbox::RunFencePhase;
using blackbox::RunManifest;
using blackbox::RunScopedCorrelationId;
using blackbox::SessionMarkerEntry;
using Json = nlohmann::json;

constexpr std::string_view run_id = "00112233-4455-6677-8899-aabbccddeeff";
constexpr std::string_view session_id = "ycsb/worker/1";

[[nodiscard]] RunManifest manifest() {
  return RunManifest{
      .run_id = std::string(run_id),
      .backend = "mariadb",
      .backend_source_id = "mariadb-main",
      .declared_isolation = "REPEATABLE_READ",
      .benchmark = "ycsb",
      .expected_session_ids = {std::string(session_id)},
      .application_table_allowlist = {"benchbase.usertable"},
      .transaction_marker_table = "benchbase.session_txn",
      .control_table_allowlist = {"benchbase.checker_run_fence"},
      .result_scope = "point_transaction_soww",
  };
}

[[nodiscard]] CdcTransaction control(const std::uint64_t cdc_seq,
                                     const RunFencePhase phase) {
  return CdcTransaction{
      .cdc_seq = cdc_seq,
      .kind = CdcTransactionKind::RunControl,
      .fence_phase = phase,
      .correlation_id = std::nullopt,
      .writes = {},
      .begin_stream_id = std::to_string(cdc_seq + 1U) + "-0",
      .end_stream_id = std::to_string(cdc_seq + 1U) + "-2",
  };
}

[[nodiscard]] CdcTransaction workload(const std::uint64_t cdc_seq,
                                      const std::uint64_t txn_id) {
  return CdcTransaction{
      .cdc_seq = cdc_seq,
      .kind = CdcTransactionKind::WorkloadMarked,
      .fence_phase = std::nullopt,
      .correlation_id =
          RunScopedCorrelationId{
              .run_id = std::string(run_id),
              .session_id = std::string(session_id),
              .txn_id = txn_id,
          },
      .writes = {},
      .begin_stream_id = std::to_string(cdc_seq + 1U) + "-0",
      .end_stream_id = std::to_string(cdc_seq + 1U) + "-2",
  };
}

[[nodiscard]] std::string encodeBase64(const CanonicalBytes &bytes) {
  constexpr std::string_view alphabet =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string result;
  result.reserve((bytes.size() + 2U) / 3U * 4U);
  for (std::size_t index = 0; index < bytes.size(); index += 3U) {
    const auto first = std::to_integer<unsigned int>(bytes[index]);
    const bool has_second = index + 1U < bytes.size();
    const bool has_third = index + 2U < bytes.size();
    const auto second =
        has_second ? std::to_integer<unsigned int>(bytes[index + 1U]) : 0U;
    const auto third =
        has_third ? std::to_integer<unsigned int>(bytes[index + 2U]) : 0U;
    result.push_back(alphabet[(first >> 2U) & 0x3fU]);
    result.push_back(
        alphabet[((first & 0x03U) << 4U) | ((second >> 4U) & 0x0fU)]);
    result.push_back(
        has_second
            ? alphabet[((second & 0x0fU) << 2U) | ((third >> 6U) & 0x03U)]
            : '=');
    result.push_back(has_third ? alphabet[third & 0x3fU] : '=');
  }
  return result;
}

[[nodiscard]] std::string uuid(const std::uint64_t sequence,
                               const char prefix) {
  std::ostringstream result;
  result << prefix << prefix << prefix << prefix << prefix << prefix << prefix
         << prefix << '-' << prefix << prefix << prefix << prefix << '-' << '4'
         << prefix << prefix << prefix << '-' << '8' << prefix << prefix
         << prefix << '-' << std::hex << std::setfill('0') << std::setw(12)
         << sequence;
  return result.str();
}

[[nodiscard]] Json readJson(const ClientRead &read) {
  Json result{{"event_seq", read.event_seq},
              {"key_bytes_base64", encodeBase64(read.key)},
              {"value_state",
               read.state == ClientValueState::Present ? "present" : "absent"},
              {"source", read.source == ClientReadSource::Snapshot ? "snapshot"
                                                                   : "local"}};
  if (read.value.has_value()) {
    result["value_bytes_base64"] = encodeBase64(*read.value);
  }
  return result;
}

[[nodiscard]] std::string
clientPayload(const std::uint64_t sequence,
              const std::vector<ClientRead> &reads = {},
              const std::optional<std::uint64_t> txn_id = std::nullopt) {
  Json read_array = Json::array();
  for (const auto &read : reads) {
    read_array.push_back(readJson(read));
  }
  return Json{
      {"kind", "client_txn"},
      {"run_id", run_id},
      {"session_id", session_id},
      {"session_seq", sequence},
      {"logical_txn_id", uuid(sequence, '1')},
      {"attempt_id", uuid(sequence, '2')},
      {"txn_type", "ycsb_read"},
      {"correlation_kind", "session_txn"},
      {"correlation_id", Json{{"run_id", run_id},
                              {"session_id", session_id},
                              {"txn_id", txn_id.value_or(sequence)}}},
      {"read_count", reads.size()},
      {"read_digest", blackbox::computeClientReadDigest(reads)},
      {"reads", std::move(read_array)},
  }
      .dump();
}

[[nodiscard]] std::string donePayloadForProgress(
    const std::uint64_t logical_transactions,
    const std::uint64_t physical_attempts,
    const std::uint64_t successful_transactions,
    const std::uint64_t terminal_aborted_transactions,
    const std::uint64_t definitively_aborted_attempts,
    const std::uint64_t retry_attempts,
    const std::vector<SessionMarkerEntry> &published_entries) {
  return Json{
      {"kind", "session_done"},
      {"run_id", run_id},
      {"session_id", session_id},
      {"final_session_seq", logical_transactions},
      {"logical_transactions_started", logical_transactions},
      {"successful_transactions", successful_transactions},
      {"terminal_aborted_transactions", terminal_aborted_transactions},
      {"physical_attempts_started", physical_attempts},
      {"definitively_aborted_attempts", definitively_aborted_attempts},
      {"published_transactions", published_entries.size()},
      {"retry_attempts", retry_attempts},
      {"ambiguous_attempts", 0},
      {"rolling_correlation_digest",
       blackbox::computeRollingSessionMarkerDigest(run_id, session_id,
                                                   published_entries)},
  }
      .dump();
}

[[nodiscard]] std::string bootstrapPayload() {
  return Json{
      {"kind", "bootstrap_manifest"},
      {"run_id", run_id},
      {"table_row_counts",
       Json::array({Json{{"table", "benchbase.usertable"}, {"row_count", 0}}})},
      {"canonical_state_digest",
       "944dd6136dbad7047e31af3b6419eb6a632d4dea64bec8159e93307163810bc2"},
  }
      .dump();
}

[[nodiscard]] std::string manifestPayload() {
  return Json{
      {"kind", "run_manifest"},
      {"run_id", run_id},
      {"backend", "mariadb"},
      {"backend_source_id", "mariadb-main"},
      {"declared_isolation", "REPEATABLE_READ"},
      {"benchmark", "ycsb"},
      {"expected_session_ids", Json::array({session_id})},
      {"application_table_allowlist", Json::array({"benchbase.usertable"})},
      {"transaction_marker_table", "benchbase.session_txn"},
      {"control_table_allowlist", Json::array({"benchbase.checker_run_fence"})},
      {"result_scope", "point_transaction_soww"},
  }
      .dump();
}

[[nodiscard]] std::string
donePayload(const std::uint64_t count,
            const std::optional<std::string> &digest_override = std::nullopt) {
  std::vector<SessionMarkerEntry> entries;
  for (std::uint64_t sequence = 1U; sequence <= count; ++sequence) {
    entries.push_back(
        SessionMarkerEntry{.session_seq = sequence, .txn_id = sequence});
  }
  const auto digest = digest_override.value_or(
      blackbox::computeRollingSessionMarkerDigest(run_id, session_id, entries));
  return Json{
      {"kind", "session_done"},
      {"run_id", run_id},
      {"session_id", session_id},
      {"final_session_seq", count},
      {"logical_transactions_started", count},
      {"successful_transactions", count},
      {"terminal_aborted_transactions", 0},
      {"physical_attempts_started", count},
      {"definitively_aborted_attempts", 0},
      {"published_transactions", count},
      {"retry_attempts", 0},
      {"ambiguous_attempts", 0},
      {"rolling_correlation_digest", digest},
  }
      .dump();
}

[[nodiscard]] std::string emptyDonePayload(const std::string_view session) {
  return Json{
      {"kind", "session_done"},
      {"run_id", run_id},
      {"session_id", session},
      {"final_session_seq", 0},
      {"logical_transactions_started", 0},
      {"successful_transactions", 0},
      {"terminal_aborted_transactions", 0},
      {"physical_attempts_started", 0},
      {"definitively_aborted_attempts", 0},
      {"published_transactions", 0},
      {"retry_attempts", 0},
      {"ambiguous_attempts", 0},
      {"rolling_correlation_digest",
       blackbox::sessionMarkerDigestHex(
           blackbox::sessionMarkerDigestSeed(run_id, session))},
  }
      .dump();
}

void discardStatuses(RunController &controller) {
  (void)controller.takePendingStatuses();
}

void openWorkload(RunController &controller) {
  discardStatuses(controller);
  controller.acceptCdcTransaction(control(0U, RunFencePhase::RunStart));
  controller.acceptClientPayload(bootstrapPayload());
  controller.acceptCdcTransaction(control(1U, RunFencePhase::LoadFinished));
  BLACKBOX_CHECK(controller.joinerMetrics().bootstrap_state_validated);
  discardStatuses(controller);
}

[[nodiscard]] bool hasDiagnostic(const RunController &controller,
                                 const std::string_view code) {
  for (const auto &diagnostic : controller.diagnostics()) {
    if (diagnostic.code == code) {
      return true;
    }
  }
  return false;
}

[[nodiscard]] std::size_t
finalStatusCount(const std::vector<std::string> &items) {
  std::size_t result = 0;
  for (const auto &item : items) {
    if (Json::parse(item).at("lifecycle_state") == "FINAL") {
      ++result;
    }
  }
  return result;
}

void testCleanPassRequiresDoneEndAndIdle() {
  RunController controller(manifest());
  openWorkload(controller);
  controller.acceptClientPayload(clientPayload(1U));
  controller.acceptCdcTransaction(workload(2U, 1U));
  controller.acceptClientPayload(donePayload(1U));
  controller.onInputIdle();
  BLACKBOX_CHECK(!controller.terminal());
  controller.acceptCdcTransaction(control(3U, RunFencePhase::EndFence));
  BLACKBOX_CHECK(!controller.terminal());
  controller.onInputIdle();
  BLACKBOX_CHECK(controller.terminal());
  BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Pass);
  BLACKBOX_CHECK(controller.endFenceProcessed());
  BLACKBOX_REQUIRE(controller.finalReconciliation().has_value());
  const auto &reconciliation = *controller.finalReconciliation();
  BLACKBOX_CHECK(reconciliation.evidence_reconciled);
  BLACKBOX_CHECK(reconciliation.completed_sessions == 1U);
  BLACKBOX_CHECK(reconciliation.accepted_client_transactions == 1U);
  BLACKBOX_CHECK(reconciliation.joined_workload_transactions == 1U);
  BLACKBOX_CHECK(reconciliation.bootstrap_cdc_transactions == 0U);
  BLACKBOX_CHECK(reconciliation.bootstrap_checker_transactions == 0U);
  BLACKBOX_CHECK(reconciliation.phase_anchor_transactions == 1U);
  BLACKBOX_CHECK(reconciliation.checker_consumed_transactions == 2U);
  BLACKBOX_CHECK(controller.diagnostics().empty());
  const auto statuses = controller.takePendingStatuses();
  BLACKBOX_CHECK(finalStatusCount(statuses) == 1U);
  const auto final = Json::parse(statuses.back());
  BLACKBOX_CHECK(final.at("outcome") == "PASS");
  BLACKBOX_CHECK(final.at("pending_cdc_slots") == 0U);
  controller.onInputIdle();
  controller.forceTimeoutFinal();
  BLACKBOX_CHECK(controller.takePendingStatuses().empty());
}

void testEndWithoutDoneIsInvalid() {
  RunController controller(manifest());
  openWorkload(controller);
  controller.acceptCdcTransaction(control(2U, RunFencePhase::EndFence));
  controller.onInputIdle();
  BLACKBOX_CHECK(controller.terminal());
  BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Invalid);
  BLACKBOX_CHECK(hasDiagnostic(controller, "SESSION_DONE_MISSING"));
}

void testOtherwiseCleanRunWithoutEndFenceIsInvalid() {
  RunController controller(manifest());
  openWorkload(controller);
  controller.acceptClientPayload(clientPayload(1U));
  controller.acceptCdcTransaction(workload(2U, 1U));
  controller.acceptClientPayload(donePayload(1U));
  controller.onInputIdle();
  BLACKBOX_CHECK(!controller.terminal());

  controller.forceTimeoutFinal();
  BLACKBOX_CHECK(controller.terminal());
  BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Invalid);
  BLACKBOX_CHECK(hasDiagnostic(controller, "FENCE_MISSING"));
  BLACKBOX_CHECK(!hasDiagnostic(controller, "SESSION_DONE_MISSING"));
  BLACKBOX_CHECK(!hasDiagnostic(controller, "CLIENT_HALF_MISSING"));
  BLACKBOX_CHECK(!hasDiagnostic(controller, "CDC_HALF_MISSING"));
  BLACKBOX_REQUIRE(controller.finalReconciliation().has_value());
  const auto &reconciliation = *controller.finalReconciliation();
  BLACKBOX_CHECK(!reconciliation.evidence_reconciled);
  BLACKBOX_CHECK(!reconciliation.end_fence_seen);
  BLACKBOX_CHECK(reconciliation.completed_sessions == 1U);
  BLACKBOX_CHECK(reconciliation.accepted_client_transactions == 1U);
  BLACKBOX_CHECK(reconciliation.joined_workload_transactions == 1U);
  BLACKBOX_CHECK(reconciliation.unmatched_client_records == 0U);
  BLACKBOX_CHECK(reconciliation.unmatched_workload_cdc_slots == 0U);
}

void testTimeoutAndDuplicateControlRecordsAreInvalid() {
  {
    RunController controller(manifest());
    controller.forceTimeoutFinal();
    BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Invalid);
    BLACKBOX_CHECK(hasDiagnostic(controller, "FENCE_MISSING"));
    BLACKBOX_CHECK(hasDiagnostic(controller, "BOOTSTRAP_MANIFEST_MISSING"));
    BLACKBOX_CHECK(hasDiagnostic(controller, "SESSION_DONE_MISSING"));
  }
  {
    RunController controller(manifest());
    controller.acceptClientPayload(manifestPayload());
    BLACKBOX_CHECK(hasDiagnostic(controller, "RUN_MANIFEST_DUPLICATE"));
    controller.forceTimeoutFinal();
    BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Invalid);
  }
  {
    RunController controller(manifest());
    openWorkload(controller);
    controller.acceptClientPayload(bootstrapPayload());
    controller.acceptClientPayload(donePayload(0U));
    controller.acceptCdcTransaction(control(2U, RunFencePhase::EndFence));
    controller.onInputIdle();
    BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Invalid);
    BLACKBOX_CHECK(hasDiagnostic(controller, "BOOTSTRAP_STATE_MISMATCH"));
  }
  {
    RunController controller(manifest());
    openWorkload(controller);
    controller.acceptClientPayload(
        Json{{"kind", "run_failed"},
             {"run_id", run_id},
             {"session_id", session_id},
             {"phase", "COMMIT"},
             {"reason_code", "COMMIT_OUTCOME_AMBIGUOUS"},
             {"diagnostic", "commit acknowledgement was lost"}}
            .dump());
    controller.acceptClientPayload(donePayload(0U));
    controller.acceptCdcTransaction(control(2U, RunFencePhase::EndFence));
    controller.onInputIdle();
    BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Invalid);
    BLACKBOX_REQUIRE(controller.finalReconciliation().has_value());
    BLACKBOX_CHECK(controller.finalReconciliation()->ambiguous_attempts == 1U);
  }
}

void testMissingClientDoesNotProcessEndFence() {
  RunController controller(manifest());
  openWorkload(controller);
  controller.acceptCdcTransaction(workload(2U, 1U));
  controller.acceptClientPayload(donePayload(0U));
  controller.acceptCdcTransaction(control(3U, RunFencePhase::EndFence));
  controller.onInputIdle();
  BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Invalid);
  BLACKBOX_CHECK(!controller.endFenceProcessed());
  BLACKBOX_CHECK(hasDiagnostic(controller, "CLIENT_HALF_MISSING"));
  BLACKBOX_REQUIRE(controller.finalReconciliation().has_value());
  BLACKBOX_CHECK(
      controller.finalReconciliation()->unmatched_workload_cdc_slots == 1U);
}

void testMissingCdcIsInvalidAfterOrderedEnd() {
  RunController controller(manifest());
  openWorkload(controller);
  controller.acceptClientPayload(clientPayload(1U));
  controller.acceptClientPayload(donePayload(1U));
  controller.acceptCdcTransaction(control(2U, RunFencePhase::EndFence));
  controller.onInputIdle();
  BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Invalid);
  BLACKBOX_CHECK(controller.endFenceProcessed());
  BLACKBOX_CHECK(hasDiagnostic(controller, "CDC_HALF_MISSING"));
  BLACKBOX_REQUIRE(controller.finalReconciliation().has_value());
  BLACKBOX_CHECK(controller.finalReconciliation()->unmatched_client_records ==
                 1U);
}

void testSessionDoneMismatchAndPostDoneClientAreInvalid() {
  {
    RunController controller(manifest());
    openWorkload(controller);
    controller.acceptClientPayload(clientPayload(1U));
    controller.acceptCdcTransaction(workload(2U, 1U));
    controller.acceptClientPayload(donePayload(1U, std::string(64U, '0')));
    controller.acceptCdcTransaction(control(3U, RunFencePhase::EndFence));
    controller.onInputIdle();
    BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Invalid);
    BLACKBOX_CHECK(hasDiagnostic(controller, "SESSION_DONE_MISMATCH"));
  }
  {
    RunController controller(manifest());
    openWorkload(controller);
    controller.acceptClientPayload(donePayload(0U));
    controller.acceptClientPayload(clientPayload(1U));
    controller.acceptCdcTransaction(control(2U, RunFencePhase::EndFence));
    controller.onInputIdle();
    BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Invalid);
    BLACKBOX_CHECK(hasDiagnostic(controller, "CLIENT_RECORD_AFTER_DONE"));
  }
}

void testStoppedAdmissionDoesNotInventCdcOrderFailure() {
  RunController controller(manifest());
  openWorkload(controller);
  controller.acceptClientPayload(donePayload(1U));
  BLACKBOX_CHECK(hasDiagnostic(controller, "SESSION_DONE_MISMATCH"));

  // Both transactions were assembled after the client-side failure. They are
  // counted for closure, but neither may bypass stopped admission and create a
  // false dense-sequence error inside the joiner.
  controller.acceptCdcTransaction(workload(2U, 1U));
  controller.acceptCdcTransaction(control(3U, RunFencePhase::EndFence));
  controller.onInputIdle();

  BLACKBOX_CHECK(controller.terminal());
  BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Invalid);
  BLACKBOX_CHECK(controller.endFenceObserved());
  BLACKBOX_CHECK(!hasDiagnostic(controller, "CDC_ORDER_INVALID"));
  BLACKBOX_CHECK(hasDiagnostic(controller, "SESSION_DONE_MISMATCH"));
}

void testSessionDoneRequiresFeasibleFinalAttemptHistory() {
  {
    RunController controller(manifest());
    openWorkload(controller);
    controller.acceptClientPayload(
        donePayloadForProgress(0U, 1U, 0U, 0U, 1U, 1U, {}));
    controller.acceptCdcTransaction(control(2U, RunFencePhase::EndFence));
    controller.onInputIdle();
    BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Invalid);
    BLACKBOX_CHECK(hasDiagnostic(controller, "SESSION_DONE_MISMATCH"));
  }
  {
    RunController controller(manifest());
    openWorkload(controller);
    controller.acceptClientPayload(clientPayload(1U));
    controller.acceptCdcTransaction(workload(2U, 1U));
    controller.acceptClientPayload(donePayloadForProgress(
        1U, 2U, 1U, 0U, 1U, 1U,
        {SessionMarkerEntry{.session_seq = 1U, .txn_id = 1U}}));
    controller.acceptCdcTransaction(control(3U, RunFencePhase::EndFence));
    controller.onInputIdle();
    BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Invalid);
    BLACKBOX_CHECK(hasDiagnostic(controller, "SESSION_DONE_MISMATCH"));
  }
  {
    RunController controller(manifest());
    openWorkload(controller);
    controller.acceptClientPayload(clientPayload(1U, {}, 2U));
    controller.acceptCdcTransaction(workload(2U, 2U));
    controller.acceptClientPayload(donePayloadForProgress(
        3U, 3U, 1U, 2U, 2U, 0U,
        {SessionMarkerEntry{.session_seq = 1U, .txn_id = 2U}}));
    controller.acceptCdcTransaction(control(3U, RunFencePhase::EndFence));
    controller.onInputIdle();
    BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Invalid);
    BLACKBOX_CHECK(hasDiagnostic(controller, "SESSION_DONE_MISMATCH"));
  }
  {
    RunController controller(manifest());
    openWorkload(controller);
    controller.acceptClientPayload(clientPayload(1U, {}, 2U));
    controller.acceptCdcTransaction(workload(2U, 2U));
    controller.acceptClientPayload(donePayloadForProgress(
        3U, 4U, 1U, 2U, 3U, 1U,
        {SessionMarkerEntry{.session_seq = 1U, .txn_id = 2U}}));
    controller.acceptCdcTransaction(control(3U, RunFencePhase::EndFence));
    controller.onInputIdle();
    BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Pass);
  }
}

void testViolationThenOperationalInvalidityPreservesObservation() {
  RunController controller(manifest());
  openWorkload(controller);
  std::array<std::optional<std::string>, 10> fields{};
  fields[0] = "not-in-bootstrap";
  const auto value = CanonicalYcsbCodec::encodeValue(fields);
  const ClientRead read{
      .event_seq = 0U,
      .key = CanonicalYcsbCodec::encodeKey("benchbase", "usertable", 42),
      .state = ClientValueState::Present,
      .value = value,
      .source = ClientReadSource::Snapshot,
  };
  controller.acceptClientPayload(clientPayload(1U, {read}));
  controller.acceptCdcTransaction(workload(2U, 1U));
  BLACKBOX_CHECK(controller.joinerMetrics().checker.workload_transactions ==
                 1U);
  controller.acceptClientPayload(donePayload(1U));
  controller.noteExternalFailure("OPERATIONAL", "RESOURCE_EXHAUSTED",
                                 "injected post-violation invalidity");
  controller.acceptCdcTransaction(control(3U, RunFencePhase::EndFence));
  controller.onInputIdle();
  BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Invalid);
  BLACKBOX_REQUIRE(controller.finalReconciliation().has_value());
  BLACKBOX_CHECK(!controller.finalReconciliation()->evidence_reconciled);
  BLACKBOX_CHECK(hasDiagnostic(controller, "VIOLATION_OBSERVED"));
  BLACKBOX_CHECK(hasDiagnostic(controller, "RESOURCE_EXHAUSTED"));
  const auto statuses = controller.takePendingStatuses();
  const auto final = Json::parse(statuses.back());
  BLACKBOX_CHECK(final.at("checker_observation") == "VIOLATION_OBSERVED");
  BLACKBOX_CHECK(final.at("outcome") == "INVALID");
}

void testJoinLimitAfterViolationPreservesObservation() {
  blackbox::RunControllerConfig config;
  config.joiner_limits.max_pending_client_records = 1U;
  RunController controller(manifest(), config);
  openWorkload(controller);

  std::array<std::optional<std::string>, 10> fields{};
  fields[0] = "impossible";
  const ClientRead read{
      .event_seq = 0U,
      .key = CanonicalYcsbCodec::encodeKey("benchbase", "usertable", 11),
      .state = ClientValueState::Present,
      .value = CanonicalYcsbCodec::encodeValue(fields),
      .source = ClientReadSource::Snapshot,
  };
  controller.acceptClientPayload(clientPayload(1U, {read}));
  controller.acceptCdcTransaction(workload(2U, 1U));
  BLACKBOX_CHECK(controller.joinerMetrics().checker.workload_transactions ==
                 1U);
  BLACKBOX_CHECK(hasDiagnostic(controller, "VIOLATION_OBSERVED"));

  controller.acceptClientPayload(clientPayload(2U));
  controller.acceptClientPayload(clientPayload(3U));
  BLACKBOX_CHECK(hasDiagnostic(controller, "JOIN_BUFFER_LIMIT_EXCEEDED"));
  controller.acceptClientPayload(donePayload(2U));
  controller.acceptCdcTransaction(control(3U, RunFencePhase::EndFence));
  controller.onInputIdle();

  BLACKBOX_CHECK(controller.terminal());
  BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Invalid);
  BLACKBOX_CHECK(hasDiagnostic(controller, "VIOLATION_OBSERVED"));
  BLACKBOX_CHECK(hasDiagnostic(controller, "JOIN_BUFFER_LIMIT_EXCEEDED"));
  const auto statuses = controller.takePendingStatuses();
  const auto final = Json::parse(statuses.back());
  BLACKBOX_CHECK(final.at("checker_observation") == "VIOLATION_OBSERVED");
  BLACKBOX_CHECK(final.at("outcome") == "INVALID");
}

void testCleanViolationFinalizesAsViolation() {
  RunController controller(manifest());
  openWorkload(controller);
  std::array<std::optional<std::string>, 10> fields{};
  fields[0] = "impossible";
  const ClientRead read{
      .event_seq = 0U,
      .key = CanonicalYcsbCodec::encodeKey("benchbase", "usertable", 7),
      .state = ClientValueState::Present,
      .value = CanonicalYcsbCodec::encodeValue(fields),
      .source = ClientReadSource::Snapshot,
  };
  controller.acceptClientPayload(clientPayload(1U, {read}));
  controller.acceptCdcTransaction(workload(2U, 1U));
  controller.acceptClientPayload(donePayload(1U));
  controller.acceptCdcTransaction(control(3U, RunFencePhase::EndFence));
  controller.onInputIdle();
  BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Violation);
  BLACKBOX_REQUIRE(controller.finalReconciliation().has_value());
  BLACKBOX_CHECK(controller.finalReconciliation()->evidence_reconciled);
  BLACKBOX_CHECK(hasDiagnostic(controller, "VIOLATION_OBSERVED"));
}

void testFirstViolationOwnsReservedDiagnosticSlotImmediately() {
  blackbox::RunControllerConfig config;
  config.limits.max_diagnostics = 1U;
  RunController controller(manifest(), config);
  openWorkload(controller);
  std::array<std::optional<std::string>, 10> fields{};
  fields[0] = "impossible";
  const ClientRead read{
      .event_seq = 0U,
      .key = CanonicalYcsbCodec::encodeKey("benchbase", "usertable", 9),
      .state = ClientValueState::Present,
      .value = CanonicalYcsbCodec::encodeValue(fields),
      .source = ClientReadSource::Snapshot,
  };
  controller.acceptClientPayload(clientPayload(1U, {read}));
  controller.acceptCdcTransaction(workload(2U, 1U));
  BLACKBOX_REQUIRE(controller.diagnostics().size() == 1U);
  BLACKBOX_CHECK(controller.diagnostics().front().code == "VIOLATION_OBSERVED");
  BLACKBOX_CHECK(controller.diagnostics().front().message.find(
                     "session_id=ycsb/worker/1") != std::string::npos);
  BLACKBOX_CHECK(controller.diagnostics().front().message.find(
                     "logical_txn_id=") != std::string::npos);
  BLACKBOX_CHECK(controller.diagnostics().front().message.find("attempt_id=") !=
                 std::string::npos);

  controller.acceptClientPayload(donePayload(1U));
  controller.noteExternalFailure("OPERATIONAL", "RESOURCE_EXHAUSTED",
                                 "diagnostic ledger is deliberately full");
  controller.acceptCdcTransaction(control(3U, RunFencePhase::EndFence));
  controller.onInputIdle();
  BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Invalid);
  BLACKBOX_REQUIRE(controller.diagnostics().size() == 1U);
  BLACKBOX_CHECK(controller.diagnostics().front().code == "VIOLATION_OBSERVED");
}

void testCompletedSessionTrackingUsesTerminalAudit() {
  constexpr std::size_t session_count = 4096U;
  auto many_sessions = manifest();
  many_sessions.expected_session_ids.clear();
  many_sessions.expected_session_ids.reserve(session_count);
  for (std::size_t index = 0; index < session_count; ++index) {
    many_sessions.expected_session_ids.push_back("ycsb/worker/" +
                                                 std::to_string(index + 1U));
  }
  RunController controller(std::move(many_sessions));
  for (std::size_t index = 0; index < session_count; ++index) {
    controller.acceptClientPayload(
        emptyDonePayload("ycsb/worker/" + std::to_string(index + 1U)));
  }
  controller.forceTimeoutFinal();
  BLACKBOX_REQUIRE(controller.finalReconciliation().has_value());
  BLACKBOX_CHECK(controller.finalReconciliation()->completed_sessions ==
                 session_count);
  BLACKBOX_CHECK(!hasDiagnostic(controller, "SESSION_DONE_MISSING"));
}

} // namespace

int runRunControllerTests() {
  testCleanPassRequiresDoneEndAndIdle();
  testEndWithoutDoneIsInvalid();
  testOtherwiseCleanRunWithoutEndFenceIsInvalid();
  testTimeoutAndDuplicateControlRecordsAreInvalid();
  testMissingClientDoesNotProcessEndFence();
  testMissingCdcIsInvalidAfterOrderedEnd();
  testSessionDoneMismatchAndPostDoneClientAreInvalid();
  testStoppedAdmissionDoesNotInventCdcOrderFailure();
  testSessionDoneRequiresFeasibleFinalAttemptHistory();
  testViolationThenOperationalInvalidityPreservesObservation();
  testJoinLimitAfterViolationPreservesObservation();
  testCleanViolationFinalizesAsViolation();
  testFirstViolationOwnsReservedDiagnosticSlotImmediately();
  testCompletedSessionTrackingUsesTerminalAudit();
  return 0;
}
