#include "blackbox/run_controller.h"

#include "blackbox/canonical_tpcc_codec.h"
#include "blackbox/canonical_ycsb_codec.h"
#include "blackbox/client_txn.h"
#include "blackbox/debezium_assembler.h"
#include "test_support.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <map>
#include <numeric>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
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
using blackbox::MonitorError;
using blackbox::MonitorErrorCode;
using blackbox::NormalizedValueState;
using blackbox::NormalizedWrite;
using blackbox::ProcessingMode;
using blackbox::RunController;
using blackbox::RunFencePhase;
using blackbox::RunManifest;
using blackbox::RunScopedCorrelationId;
using blackbox::SessionMarkerEntry;
using Json = nlohmann::json;

#ifndef BLACKBOX_PROTOCOL_FIXTURE_DIR
#error "BLACKBOX_PROTOCOL_FIXTURE_DIR must name the shared protocol fixtures"
#endif

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

[[nodiscard]] CdcTransaction
workload(const std::uint64_t cdc_seq, const std::uint64_t txn_id,
         std::vector<NormalizedWrite> writes = {}) {
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
      .writes = std::move(writes),
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

[[nodiscard]] RunManifest tpccManifest() {
  auto result = manifest();
  result.benchmark = "tpcc";
  result.application_table_allowlist.clear();
  for (const auto &table :
       blackbox::BenchmarkAdapterRegistry::require("tpcc").tables()) {
    result.application_table_allowlist.emplace_back(table.name);
  }
  return result;
}

[[nodiscard]] std::string tpccBootstrapPayload() {
  Json counts = Json::array();
  for (const auto &table :
       blackbox::BenchmarkAdapterRegistry::require("tpcc").tables()) {
    counts.push_back(Json{{"table", table.name}, {"row_count", 0}});
  }
  std::sort(counts.begin(), counts.end(),
            [](const Json &left, const Json &right) {
              return left.at("table").get<std::string>() <
                     right.at("table").get<std::string>();
            });
  return Json{
      {"kind", "bootstrap_manifest"},
      {"run_id", run_id},
      {"table_row_counts", std::move(counts)},
      {"canonical_state_digest",
       "944dd6136dbad7047e31af3b6419eb6a632d4dea64bec8159e93307163810bc2"},
  }
      .dump();
}

[[nodiscard]] Json loadTpccTransactionFixture() {
  std::ifstream input(std::string(BLACKBOX_PROTOCOL_FIXTURE_DIR) +
                      "/mariadb_tpcc_transactions.json");
  if (!input) {
    throw std::runtime_error(
        "failed to open captured TPC-C transaction fixture");
  }
  return Json::parse(input);
}

[[nodiscard]] Json loadTpccClientFixture() {
  std::ifstream input(std::string(BLACKBOX_PROTOCOL_FIXTURE_DIR) +
                      "/mariadb_tpcc_client_txns.json");
  if (!input) {
    throw std::runtime_error("failed to open captured TPC-C client fixture");
  }
  return Json::parse(input);
}

[[nodiscard]] std::vector<CdcTransaction>
assembleTpccFixture(const Json &fixture) {
  const auto &adapter = blackbox::BenchmarkAdapterRegistry::require("tpcc");
  std::vector<std::string> tables;
  for (const auto &table : adapter.tables()) {
    tables.emplace_back(table.name);
  }
  blackbox::DebeziumTxnAssembler assembler(blackbox::DebeziumAssemblerConfig{
      .run_id = fixture.at("run_id").get<std::string>(),
      .backend_source_id = "mariadb-main",
      .debezium_version = fixture.at("debezium_version").get<std::string>(),
      .backend_adapter = &blackbox::BackendAdapterRegistry::require("mariadb"),
      .benchmark_adapter = &adapter,
      .application_tables = std::move(tables),
      .transaction_marker_table = "benchbase.session_txn",
      .run_fence_table = "benchbase.checker_run_fence",
  });
  Json entries = fixture.at("startup_schema");
  for (const auto &transaction : fixture.at("transactions")) {
    for (const auto &entry : transaction.at("raw_entries")) {
      entries.push_back(entry);
    }
  }
  std::vector<CdcTransaction> result;
  for (const auto &entry : entries) {
    auto completed = assembler.accept(blackbox::CdcStreamRecord{
        .stream_id = entry.at(0).get<std::string>(),
        .key_json = entry.at(1).at(0).get<std::string>(),
        .envelope_json = entry.at(1).at(1).get<std::string>(),
    });
    if (completed.has_value()) {
      result.push_back(std::move(*completed));
    }
  }
  assembler.finish();
  return result;
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

void openTpccWorkload(RunController &controller) {
  discardStatuses(controller);
  controller.acceptCdcTransaction(control(0U, RunFencePhase::RunStart));
  controller.acceptClientPayload(tpccBootstrapPayload());
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

void testIncorrectInitialBootstrapManifestPreventsRelease() {
  RunController controller(manifest());
  discardStatuses(controller);
  controller.acceptCdcTransaction(control(0U, RunFencePhase::RunStart));
  controller.acceptCdcTransaction(control(1U, RunFencePhase::LoadFinished));

  // Both halves are ready behind LOAD_FINISHED. An incorrect first manifest
  // must invalidate the run without releasing its anchors or this workload.
  controller.acceptClientPayload(clientPayload(1U));
  controller.acceptCdcTransaction(workload(2U, 1U));
  auto incorrect_bootstrap = Json::parse(bootstrapPayload());
  incorrect_bootstrap["canonical_state_digest"] = std::string(64U, '0');
  controller.acceptClientPayload(incorrect_bootstrap.dump());

  BLACKBOX_CHECK(hasDiagnostic(controller, "BOOTSTRAP_STATE_MISMATCH"));
  const auto metrics = controller.joinerMetrics();
  BLACKBOX_CHECK(!metrics.bootstrap_state_validated);
  BLACKBOX_CHECK(metrics.accepted_client_transactions == 1U);
  BLACKBOX_CHECK(metrics.accepted_workload_cdc_transactions == 1U);
  BLACKBOX_CHECK(metrics.pending_cdc_slots == 2U);
  BLACKBOX_CHECK(metrics.joined_workload_transactions == 0U);
  BLACKBOX_CHECK(metrics.checker.bootstrap_transactions == 0U);
  BLACKBOX_CHECK(metrics.checker.phase_anchor_transactions == 0U);
  BLACKBOX_CHECK(metrics.checker.workload_transactions == 0U);
  BLACKBOX_CHECK(metrics.checker.consumed_transactions == 0U);

  controller.acceptClientPayload(donePayload(1U));
  controller.acceptCdcTransaction(control(3U, RunFencePhase::EndFence));
  controller.onInputIdle();

  BLACKBOX_CHECK(controller.terminal());
  BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Invalid);
  BLACKBOX_CHECK(controller.endFenceObserved());
  BLACKBOX_CHECK(!controller.endFenceProcessed());
  BLACKBOX_REQUIRE(controller.finalReconciliation().has_value());
  const auto &reconciliation = *controller.finalReconciliation();
  BLACKBOX_CHECK(!reconciliation.bootstrap_manifest_validated);
  BLACKBOX_CHECK(reconciliation.joined_workload_transactions == 0U);
  BLACKBOX_CHECK(reconciliation.bootstrap_checker_transactions == 0U);
  BLACKBOX_CHECK(reconciliation.phase_anchor_transactions == 0U);
  BLACKBOX_CHECK(reconciliation.checker_consumed_transactions == 0U);

  const auto statuses = controller.takePendingStatuses();
  BLACKBOX_CHECK(finalStatusCount(statuses) == 1U);
  for (const auto &status : statuses) {
    const auto state =
        Json::parse(status).at("lifecycle_state").get<std::string>();
    BLACKBOX_CHECK(state != "LOAD_FINISHED_SEEN");
    BLACKBOX_CHECK(state != "RUNNING");
    BLACKBOX_CHECK(state != "RECONCILED");
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

void testJoinOnlyReconcilesWouldBeViolationWithoutVerdict() {
  blackbox::RunControllerConfig config;
  config.processing_mode = ProcessingMode::JoinOnly;
  RunController controller(manifest(), config);
  openWorkload(controller);

  std::array<std::optional<std::string>, 10> fields{};
  fields[0] = "impossible";
  const ClientRead read{
      .event_seq = 0U,
      .key = CanonicalYcsbCodec::encodeKey("benchbase", "usertable", 17),
      .state = ClientValueState::Present,
      .value = CanonicalYcsbCodec::encodeValue(fields),
      .source = ClientReadSource::Snapshot,
  };
  controller.acceptClientPayload(clientPayload(1U, {read}));
  controller.acceptCdcTransaction(workload(2U, 1U));
  controller.acceptClientPayload(donePayload(1U));
  controller.acceptCdcTransaction(control(3U, RunFencePhase::EndFence));
  controller.onInputIdle();

  BLACKBOX_CHECK(controller.terminal());
  BLACKBOX_CHECK(controller.processingMode() == ProcessingMode::JoinOnly);
  BLACKBOX_CHECK(!controller.finalOutcome().has_value());
  BLACKBOX_REQUIRE(controller.finalReconciliation().has_value());
  BLACKBOX_CHECK(controller.finalReconciliation()->evidence_reconciled);
  BLACKBOX_CHECK(controller.diagnostics().empty());

  const auto metrics = controller.joinerMetrics();
  BLACKBOX_CHECK(metrics.released_bootstrap_transactions == 0U);
  BLACKBOX_CHECK(metrics.released_phase_anchor_transactions == 1U);
  BLACKBOX_CHECK(metrics.released_workload_transactions == 1U);
  BLACKBOX_CHECK(metrics.joined_read_events == 1U);
  BLACKBOX_CHECK(metrics.checker.consumed_transactions == 0U);
  BLACKBOX_CHECK(metrics.checker.consume_total_nanoseconds == 0U);
  BLACKBOX_CHECK(metrics.checker.mapped_identity_records == 0U);
  BLACKBOX_CHECK(metrics.checker.engine.retained_state_bytes == 0U);

  const auto statuses = controller.takePendingStatuses();
  for (const auto &status_text : statuses) {
    const auto status = Json::parse(status_text);
    BLACKBOX_CHECK(status.at("lifecycle_state") != "FINAL");
    BLACKBOX_CHECK(status.at("outcome") == "RUNNING");
    BLACKBOX_CHECK(status.at("checker_observation") == "NOT_STARTED");
    BLACKBOX_CHECK(status_text.find("PASS") == std::string::npos);
    BLACKBOX_CHECK(status_text.find("VIOLATION") == std::string::npos);
    BLACKBOX_CHECK(status_text.find("INVALID") == std::string::npos);
  }
  const auto report = controller.finalReport();
  BLACKBOX_CHECK(report.find("PASS") == std::string::npos);
  BLACKBOX_CHECK(report.find("VIOLATION") == std::string::npos);
  BLACKBOX_CHECK(report.find("INVALID") == std::string::npos);
  BLACKBOX_CHECK(report.find("completion_status=completed") !=
                 std::string::npos);
}

void testJoinOnlyFailureHasNoAuditedOutcome() {
  blackbox::RunControllerConfig config;
  config.processing_mode = ProcessingMode::JoinOnly;
  RunController controller(manifest(), config);
  openWorkload(controller);
  controller.acceptCdcTransaction(control(2U, RunFencePhase::EndFence));
  controller.onInputIdle();

  BLACKBOX_CHECK(controller.terminal());
  BLACKBOX_CHECK(!controller.finalOutcome().has_value());
  BLACKBOX_REQUIRE(controller.finalReconciliation().has_value());
  BLACKBOX_CHECK(!controller.finalReconciliation()->evidence_reconciled);
  BLACKBOX_CHECK(hasDiagnostic(controller, "SESSION_DONE_MISSING"));
  BLACKBOX_CHECK(controller.finalReport().find("completion_status=failed") !=
                 std::string::npos);
  const auto statuses = controller.takePendingStatuses();
  BLACKBOX_CHECK(finalStatusCount(statuses) == 0U);
  for (const auto &status_text : statuses) {
    const auto status = Json::parse(status_text);
    BLACKBOX_CHECK(status.at("checker_observation") == "NOT_STARTED");
    BLACKBOX_CHECK(status_text.find("PASS") == std::string::npos);
    BLACKBOX_CHECK(status_text.find("VIOLATION") == std::string::npos);
    BLACKBOX_CHECK(status_text.find("INVALID") == std::string::npos);
  }
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

void testMonitorOwnedCapabilitiesAdmitTpcc() {
  auto tpcc = manifest();
  tpcc.benchmark = "tpcc";
  tpcc.expected_session_ids = {"tpcc/worker/1"};
  tpcc.application_table_allowlist = {
      "benchbase.warehouse", "benchbase.item",      "benchbase.stock",
      "benchbase.district",  "benchbase.customer",  "benchbase.history",
      "benchbase.oorder",    "benchbase.new_order", "benchbase.order_line",
  };
  auto read_committed = tpcc;
  read_committed.declared_isolation = "READ_COMMITTED";
  try {
    (void)RunController(std::move(read_committed));
    BLACKBOX_CHECK(false);
  } catch (const MonitorError &error) {
    BLACKBOX_CHECK(error.code() == MonitorErrorCode::IsolationMismatch);
  }
  RunController controller(std::move(tpcc));
  BLACKBOX_CHECK(!controller.terminal());
}

void testAdapterManifestSemanticsAreValidatedAtAdmission() {
  {
    auto unknown = manifest();
    unknown.benchmark = "unknown";
    try {
      (void)RunController(std::move(unknown));
      BLACKBOX_CHECK(false);
    } catch (const MonitorError &error) {
      BLACKBOX_CHECK(error.code() == MonitorErrorCode::ConfigInvalid);
    }
  }
  {
    auto wrong_roster = manifest();
    wrong_roster.application_table_allowlist = {"benchbase.other"};
    try {
      (void)RunController(std::move(wrong_roster));
      BLACKBOX_CHECK(false);
    } catch (const MonitorError &error) {
      BLACKBOX_CHECK(error.code() == MonitorErrorCode::UnexpectedTable);
    }
  }
}

void testTpccControllerUsesGenericJoinAndReconciliation() {
  RunController controller(tpccManifest());
  openTpccWorkload(controller);
  auto client = Json::parse(clientPayload(1U, {}, 2U));
  client["txn_type"] = "NewOrder";
  controller.acceptClientPayload(client.dump());
  const auto encoded = blackbox::CanonicalTpccCodec::encodeRow(
      "benchbase", "new_order",
      Json{{"no_w_id", 1}, {"no_d_id", 2}, {"no_o_id", 3}});
  controller.acceptCdcTransaction(
      workload(2U, 2U,
               {NormalizedWrite{.key = encoded.key,
                                .state = NormalizedValueState::Present,
                                .value = encoded.value}}));
  controller.acceptClientPayload(donePayloadForProgress(
      1U, 2U, 1U, 0U, 1U, 1U,
      {SessionMarkerEntry{.session_seq = 1U, .txn_id = 2U}}));
  controller.acceptCdcTransaction(control(3U, RunFencePhase::EndFence));
  controller.onInputIdle();
  BLACKBOX_CHECK(controller.terminal());
  BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Pass);
  BLACKBOX_REQUIRE(controller.finalReconciliation().has_value());
  BLACKBOX_CHECK(
      controller.finalReconciliation()->joined_workload_transactions == 1U);
  BLACKBOX_CHECK(controller.joinerMetrics().joined_write_events == 1U);
  BLACKBOX_CHECK(controller.joinerMetrics().unmatched_client_records == 0U);
  BLACKBOX_CHECK(controller.joinerMetrics().unmatched_workload_cdc_slots == 0U);
}

void testCapturedPaymentRetryReconciliation() {
  const auto fixture = loadTpccTransactionFixture();
  const auto client_fixture = loadTpccClientFixture();
  auto transactions = assembleTpccFixture(fixture);
  BLACKBOX_REQUIRE(transactions.size() == 12U);
  const auto found = std::find_if(
      client_fixture.at("semantic_transactions").begin(),
      client_fixture.at("semantic_transactions").end(), [](const Json &entry) {
        return entry.at("client_txn").at("txn_type") == "Payment";
      });
  BLACKBOX_REQUIRE(found != client_fixture.at("semantic_transactions").end());
  BLACKBOX_CHECK(found->at("cdc_transaction") == "payment_shape");
  auto client = found->at("client_txn");
  BLACKBOX_CHECK(client.at("read_count") == 3U);
  BLACKBOX_CHECK(client.at("reads")[0].at("source") == "local");
  BLACKBOX_CHECK(client.at("reads")[1].at("source") == "local");
  BLACKBOX_CHECK(client.at("reads")[2].at("source") == "snapshot");

  client["correlation_id"]["txn_id"] = 2;
  BLACKBOX_REQUIRE(transactions[3].correlation_id.has_value());
  transactions[3].correlation_id->txn_id = 2U;
  BLACKBOX_REQUIRE(transactions[3].writes.size() == 4U);

  RunController controller(blackbox::ProtocolRecordParser{}.parseRunManifest(
      client_fixture.at("run_manifest").dump()));
  discardStatuses(controller);
  controller.acceptCdcTransaction(std::move(transactions[0]));
  controller.acceptCdcTransaction(std::move(transactions[1]));
  controller.acceptClientPayload(fixture.at("bootstrap_manifest").dump());
  controller.acceptCdcTransaction(std::move(transactions[2]));
  BLACKBOX_CHECK(controller.joinerMetrics().bootstrap_state_validated);
  controller.acceptClientPayload(client.dump());
  controller.acceptCdcTransaction(std::move(transactions[3]));

  const auto run = client.at("run_id").get<std::string>();
  const auto session = client.at("session_id").get<std::string>();
  const std::array marker_entries = {
      SessionMarkerEntry{.session_seq = 1U, .txn_id = 2U}};
  controller.acceptClientPayload(Json{
      {"kind", "session_done"},
      {"run_id", run},
      {"session_id", session},
      {"final_session_seq", 1},
      {"logical_transactions_started", 1},
      {"successful_transactions", 1},
      {"terminal_aborted_transactions", 0},
      {"physical_attempts_started", 2},
      {"definitively_aborted_attempts", 1},
      {"published_transactions", 1},
      {"retry_attempts", 1},
      {"ambiguous_attempts", 0},
      {"rolling_correlation_digest",
       blackbox::computeRollingSessionMarkerDigest(
           run, session,
           marker_entries)}}.dump());
  transactions[11].cdc_seq = 4U;
  controller.acceptCdcTransaction(std::move(transactions[11]));
  controller.onInputIdle();

  BLACKBOX_CHECK(controller.terminal());
  BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Pass);
  BLACKBOX_REQUIRE(controller.finalReconciliation().has_value());
  const auto &reconciliation = *controller.finalReconciliation();
  BLACKBOX_CHECK(reconciliation.evidence_reconciled);
  BLACKBOX_CHECK(reconciliation.accepted_client_transactions == 1U);
  BLACKBOX_CHECK(reconciliation.joined_workload_transactions == 1U);
  BLACKBOX_CHECK(reconciliation.unmatched_client_records == 0U);
  BLACKBOX_CHECK(reconciliation.unmatched_workload_cdc_slots == 0U);
  const auto metrics = controller.joinerMetrics();
  BLACKBOX_CHECK(metrics.joined_read_events == 3U);
  BLACKBOX_CHECK(metrics.joined_write_events == 4U);
  BLACKBOX_CHECK(metrics.client_first_matches == 1U);
  BLACKBOX_CHECK(metrics.cdc_first_matches == 0U);
}

void testCapturedDeliveryRetryReconciliation() {
  const auto fixture = loadTpccTransactionFixture();
  const auto client_fixture = loadTpccClientFixture();
  auto transactions = assembleTpccFixture(fixture);
  BLACKBOX_REQUIRE(transactions.size() == 12U);
  const auto found = std::find_if(
      client_fixture.at("semantic_transactions").begin(),
      client_fixture.at("semantic_transactions").end(), [](const Json &entry) {
        return entry.at("client_txn").at("txn_type") == "Delivery";
      });
  BLACKBOX_REQUIRE(found != client_fixture.at("semantic_transactions").end());
  BLACKBOX_CHECK(found->at("cdc_transaction") == "delivery_shape");
  auto client = found->at("client_txn");
  BLACKBOX_CHECK(client.at("read_count") == 2U);
  BLACKBOX_CHECK(client.at("reads")[0].at("source") == "snapshot");
  BLACKBOX_CHECK(client.at("reads")[1].at("source") == "snapshot");

  client["session_seq"] = 1;
  client["correlation_id"]["txn_id"] = 2;
  BLACKBOX_REQUIRE(transactions[5].correlation_id.has_value());
  transactions[5].cdc_seq = 3U;
  transactions[5].correlation_id->txn_id = 2U;
  BLACKBOX_REQUIRE(transactions[5].writes.size() == 4U);

  RunController controller(blackbox::ProtocolRecordParser{}.parseRunManifest(
      client_fixture.at("run_manifest").dump()));
  discardStatuses(controller);
  controller.acceptCdcTransaction(std::move(transactions[0]));
  controller.acceptCdcTransaction(std::move(transactions[1]));
  controller.acceptClientPayload(fixture.at("bootstrap_manifest").dump());
  controller.acceptCdcTransaction(std::move(transactions[2]));
  BLACKBOX_CHECK(controller.joinerMetrics().bootstrap_state_validated);
  controller.acceptCdcTransaction(std::move(transactions[5]));
  controller.acceptClientPayload(client.dump());

  const auto run = client.at("run_id").get<std::string>();
  const auto session = client.at("session_id").get<std::string>();
  const std::array marker_entries = {
      SessionMarkerEntry{.session_seq = 1U, .txn_id = 2U}};
  controller.acceptClientPayload(Json{
      {"kind", "session_done"},
      {"run_id", run},
      {"session_id", session},
      {"final_session_seq", 1},
      {"logical_transactions_started", 1},
      {"successful_transactions", 1},
      {"terminal_aborted_transactions", 0},
      {"physical_attempts_started", 2},
      {"definitively_aborted_attempts", 1},
      {"published_transactions", 1},
      {"retry_attempts", 1},
      {"ambiguous_attempts", 0},
      {"rolling_correlation_digest",
       blackbox::computeRollingSessionMarkerDigest(
           run, session,
           marker_entries)}}.dump());
  transactions[11].cdc_seq = 4U;
  controller.acceptCdcTransaction(std::move(transactions[11]));
  controller.onInputIdle();

  BLACKBOX_CHECK(controller.terminal());
  BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Pass);
  BLACKBOX_REQUIRE(controller.finalReconciliation().has_value());
  const auto &reconciliation = *controller.finalReconciliation();
  BLACKBOX_CHECK(reconciliation.evidence_reconciled);
  BLACKBOX_CHECK(reconciliation.accepted_client_transactions == 1U);
  BLACKBOX_CHECK(reconciliation.joined_workload_transactions == 1U);
  BLACKBOX_CHECK(reconciliation.unmatched_client_records == 0U);
  BLACKBOX_CHECK(reconciliation.unmatched_workload_cdc_slots == 0U);
  const auto metrics = controller.joinerMetrics();
  BLACKBOX_CHECK(metrics.joined_read_events == 2U);
  BLACKBOX_CHECK(metrics.joined_write_events == 4U);
  BLACKBOX_CHECK(metrics.client_first_matches == 0U);
  BLACKBOX_CHECK(metrics.cdc_first_matches == 1U);
}

void testDeliveryEmptyAndTerminalFailureAlgebra() {
  {
    RunController controller(tpccManifest());
    openTpccWorkload(controller);
    auto client = Json::parse(clientPayload(1U));
    client["txn_type"] = "Delivery";
    controller.acceptClientPayload(client.dump());
    controller.acceptCdcTransaction(workload(2U, 1U));
    controller.acceptClientPayload(donePayload(1U));
    controller.acceptCdcTransaction(control(3U, RunFencePhase::EndFence));
    controller.onInputIdle();
    BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Pass);
    BLACKBOX_REQUIRE(controller.finalReconciliation().has_value());
    BLACKBOX_CHECK(
        controller.finalReconciliation()->joined_workload_transactions == 1U);
    BLACKBOX_CHECK(
        controller.joinerMetrics().marker_only_workload_transactions == 1U);
    BLACKBOX_CHECK(controller.joinerMetrics().joined_read_events == 0U);
    BLACKBOX_CHECK(controller.joinerMetrics().joined_write_events == 0U);
  }
  {
    RunController controller(tpccManifest());
    openTpccWorkload(controller);
    controller.acceptClientPayload(
        donePayloadForProgress(1U, 1U, 0U, 1U, 1U, 0U, {}));
    controller.acceptCdcTransaction(control(2U, RunFencePhase::EndFence));
    controller.onInputIdle();
    BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Pass);
    BLACKBOX_REQUIRE(controller.finalReconciliation().has_value());
    BLACKBOX_CHECK(
        controller.finalReconciliation()->accepted_client_transactions == 0U);
    BLACKBOX_CHECK(
        controller.finalReconciliation()->joined_workload_transactions == 0U);
    BLACKBOX_CHECK(controller.joinerMetrics().unmatched_client_records == 0U);
    BLACKBOX_CHECK(controller.joinerMetrics().unmatched_workload_cdc_slots ==
                   0U);
  }
  {
    RunController controller(tpccManifest());
    openTpccWorkload(controller);
    controller.acceptClientPayload(
        Json{{"kind", "run_failed"},
             {"run_id", run_id},
             {"session_id", session_id},
             {"phase", "ATTEMPT_BODY"},
             {"reason_code", "INTERNAL_ERROR"},
             {"diagnostic", "Delivery retry signal escaped its boundary"}}
            .dump());
    controller.acceptClientPayload(
        donePayloadForProgress(1U, 1U, 0U, 1U, 1U, 0U, {}));
    controller.acceptCdcTransaction(control(2U, RunFencePhase::EndFence));
    controller.onInputIdle();
    BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Invalid);
    BLACKBOX_CHECK(hasDiagnostic(controller, "INTERNAL_ERROR"));
  }
}

void testStockLevelFocusedReconciliationAndMalformedEvidence() {
  const auto fixture = loadTpccTransactionFixture();
  const auto client_fixture = loadTpccClientFixture();
  const auto run_manifest = blackbox::ProtocolRecordParser{}.parseRunManifest(
      client_fixture.at("run_manifest").dump());
  const auto &adapter = blackbox::BenchmarkAdapterRegistry::require("tpcc");
  const blackbox::ClientTxnParser parser(
      run_manifest.run_id, run_manifest.expected_session_ids, adapter);
  const auto semantic = [&](const std::string_view txn_type) {
    const auto &transactions = client_fixture.at("semantic_transactions");
    const auto found = std::find_if(
        transactions.begin(), transactions.end(), [&](const Json &entry) {
          return entry.at("client_txn").at("txn_type") == txn_type;
        });
    return found == transactions.end() ? nullptr : &*found;
  };
  const auto *payment_entry = semantic("Payment");
  const auto *new_order_entry = semantic("NewOrder");
  const auto *stock_level_entry = semantic("StockLevel");
  BLACKBOX_REQUIRE(payment_entry != nullptr);
  BLACKBOX_REQUIRE(new_order_entry != nullptr);
  BLACKBOX_REQUIRE(stock_level_entry != nullptr);
  BLACKBOX_CHECK(stock_level_entry->at("cdc_transaction") ==
                 "stock_level_marker_only");
  const auto parsed_stock_level =
      parser.parse(stock_level_entry->at("client_txn").dump());
  BLACKBOX_REQUIRE(parsed_stock_level.reads.size() == 1U);
  BLACKBOX_CHECK(
      parsed_stock_level.read_digest ==
      "376b04a2c33d3bc987f6a1383f3f0e0a2bd6a24e4fed62095a4cbb37e9b291ca");

  const auto run_case = [&](const std::vector<ClientRead> &stock_level_reads,
                            const FinalOutcome expected) {
    auto transactions = assembleTpccFixture(fixture);
    BLACKBOX_REQUIRE(transactions.size() == 12U);
    auto stock_level_client = stock_level_entry->at("client_txn");
    stock_level_client["reads"] = Json::array();
    for (const auto &read : stock_level_reads) {
      stock_level_client["reads"].push_back(readJson(read));
    }
    stock_level_client["read_count"] = stock_level_reads.size();
    stock_level_client["read_digest"] =
        blackbox::computeClientReadDigest(stock_level_reads);

    RunController controller(run_manifest);
    discardStatuses(controller);
    controller.acceptCdcTransaction(std::move(transactions[0]));
    controller.acceptCdcTransaction(std::move(transactions[1]));
    controller.acceptClientPayload(fixture.at("bootstrap_manifest").dump());
    controller.acceptCdcTransaction(std::move(transactions[2]));
    controller.acceptClientPayload(payment_entry->at("client_txn").dump());
    controller.acceptCdcTransaction(std::move(transactions[3]));
    controller.acceptCdcTransaction(std::move(transactions[4]));
    controller.acceptClientPayload(new_order_entry->at("client_txn").dump());
    transactions[10].cdc_seq = 5U;
    controller.acceptClientPayload(stock_level_client.dump());
    controller.acceptCdcTransaction(std::move(transactions[10]));

    const auto run = stock_level_client.at("run_id").get<std::string>();
    const auto session = stock_level_client.at("session_id").get<std::string>();
    const std::array marker_entries = {
        SessionMarkerEntry{.session_seq = 1U, .txn_id = 1U},
        SessionMarkerEntry{.session_seq = 2U, .txn_id = 2U},
        SessionMarkerEntry{.session_seq = 8U, .txn_id = 8U}};
    controller.acceptClientPayload(Json{
        {"kind", "session_done"},
        {"run_id", run},
        {"session_id", session},
        {"final_session_seq", 8},
        {"logical_transactions_started", 8},
        {"successful_transactions", 3},
        {"terminal_aborted_transactions", 5},
        {"physical_attempts_started", 8},
        {"definitively_aborted_attempts", 5},
        {"published_transactions", 3},
        {"retry_attempts", 0},
        {"ambiguous_attempts", 0},
        {"rolling_correlation_digest",
         blackbox::computeRollingSessionMarkerDigest(
             run, session,
             marker_entries)}}.dump());
    transactions[11].cdc_seq = 6U;
    controller.acceptCdcTransaction(std::move(transactions[11]));
    controller.onInputIdle();

    BLACKBOX_CHECK(controller.terminal());
    BLACKBOX_CHECK(controller.finalOutcome() == expected);
    BLACKBOX_REQUIRE(controller.finalReconciliation().has_value());
    const auto &reconciliation = *controller.finalReconciliation();
    BLACKBOX_CHECK(reconciliation.evidence_reconciled);
    BLACKBOX_CHECK(reconciliation.accepted_client_transactions == 3U);
    BLACKBOX_CHECK(reconciliation.joined_workload_transactions == 3U);
    BLACKBOX_CHECK(reconciliation.unmatched_client_records == 0U);
    BLACKBOX_CHECK(reconciliation.unmatched_workload_cdc_slots == 0U);
    const auto metrics = controller.joinerMetrics();
    BLACKBOX_CHECK(metrics.marker_only_workload_transactions == 1U);
    BLACKBOX_CHECK(metrics.joined_write_events == 10U);
    if (expected == FinalOutcome::Pass) {
      BLACKBOX_CHECK(metrics.joined_read_events == 7U);
      BLACKBOX_CHECK(!hasDiagnostic(controller, "VIOLATION_OBSERVED"));
    } else {
      BLACKBOX_CHECK(hasDiagnostic(controller, "VIOLATION_OBSERVED"));
    }
  };

  run_case(parsed_stock_level.reads, FinalOutcome::Pass);

  auto absent_district = parsed_stock_level.reads;
  absent_district[0].state = ClientValueState::Absent;
  absent_district[0].value = std::nullopt;
  run_case(absent_district, FinalOutcome::Violation);
}

void testCapturedTpccFullMixAndStructuralFailures() {
  enum class Mutation {
    None,
    DropClient,
    DropCdc,
    WrongTypedValue,
    ResourceExhaustion,
  };
  struct Result {
    FinalOutcome outcome = FinalOutcome::Invalid;
    blackbox::ReconciliationSummary reconciliation;
    blackbox::TransactionJoinerMetrics metrics;
    std::vector<blackbox::RunDiagnostic> diagnostics;
  };

  const auto fixture = loadTpccTransactionFixture();
  const auto client_fixture = loadTpccClientFixture();
  const auto manifest = blackbox::ProtocolRecordParser{}.parseRunManifest(
      client_fixture.at("run_manifest").dump());
  const auto &adapter = blackbox::BenchmarkAdapterRegistry::require("tpcc");
  BLACKBOX_REQUIRE(manifest.expected_session_ids.size() == 1U);
  BLACKBOX_CHECK(manifest.expected_session_ids[0] == "tpcc/worker/1");
  BLACKBOX_REQUIRE(manifest.application_table_allowlist.size() == 9U);
  std::set<std::string> manifest_tables(
      manifest.application_table_allowlist.begin(),
      manifest.application_table_allowlist.end());
  std::set<std::string> adapter_tables;
  for (const auto &table : adapter.tables()) {
    adapter_tables.emplace(table.name);
  }
  BLACKBOX_CHECK(manifest_tables == adapter_tables);
  std::set<std::string> bootstrap_tables;
  for (const auto &entry :
       fixture.at("bootstrap_manifest").at("table_row_counts")) {
    BLACKBOX_CHECK(entry.at("row_count").get<std::uint64_t>() > 0U);
    bootstrap_tables.insert(entry.at("table").get<std::string>());
  }
  BLACKBOX_CHECK(bootstrap_tables == adapter_tables);

  std::set<std::string> semantic_types;
  for (const auto &entry : client_fixture.at("semantic_transactions")) {
    semantic_types.insert(
        entry.at("client_txn").at("txn_type").get<std::string>());
  }
  BLACKBOX_CHECK(semantic_types ==
                 std::set<std::string>({"NewOrder", "Payment", "OrderStatus",
                                        "Delivery", "StockLevel"}));
  std::map<std::string, std::size_t> published_per_type;
  for (const auto field : {"semantic_transactions", "synthetic_join_clients"}) {
    for (const auto &entry : client_fixture.at(field)) {
      ++published_per_type
          [entry.at("client_txn").at("txn_type").get<std::string>()];
    }
  }
  const std::map<std::string, std::size_t> expected_fixture_type_counts = {
      {"Delivery", 1U},
      {"NewOrder", 2U},
      {"OrderStatus", 1U},
      {"Payment", 3U},
      {"StockLevel", 1U}};
  BLACKBOX_CHECK(published_per_type == expected_fixture_type_counts);

  const auto run = [&](const Mutation mutation) {
    auto transactions = assembleTpccFixture(fixture);
    if (transactions.size() != 12U) {
      throw std::runtime_error("captured TPC-C transaction roster changed");
    }
    std::map<std::string, Json> clients_by_cdc_transaction;
    for (const auto field :
         {"semantic_transactions", "synthetic_join_clients"}) {
      for (const auto &entry : client_fixture.at(field)) {
        clients_by_cdc_transaction.emplace(
            entry.at("cdc_transaction").get<std::string>(),
            entry.at("client_txn"));
      }
    }
    if (clients_by_cdc_transaction.size() != 8U) {
      throw std::runtime_error("captured TPC-C client roster changed");
    }
    if (mutation == Mutation::WrongTypedValue) {
      clients_by_cdc_transaction.at(
          "payment_shape")["reads"][0]["value_bytes_base64"] = "AA==";
    }

    blackbox::RunControllerConfig config;
    if (mutation == Mutation::ResourceExhaustion) {
      config.joiner_limits.max_pending_client_records = 1U;
    }
    RunController controller(manifest, config);
    discardStatuses(controller);
    controller.acceptCdcTransaction(std::move(transactions[0]));
    controller.acceptCdcTransaction(std::move(transactions[1]));
    controller.acceptClientPayload(fixture.at("bootstrap_manifest").dump());
    controller.acceptCdcTransaction(std::move(transactions[2]));

    if (mutation == Mutation::ResourceExhaustion) {
      for (std::size_t index = 3U; index <= 10U; ++index) {
        const auto transaction_name =
            fixture.at("transactions")[index].at("name").get<std::string>();
        controller.acceptClientPayload(
            clients_by_cdc_transaction.at(transaction_name).dump());
      }
      for (std::size_t index = 3U; index <= 10U; ++index) {
        controller.acceptCdcTransaction(std::move(transactions[index]));
      }
    } else {
      bool cdc_dropped = false;
      for (std::size_t index = 3U; index <= 10U; ++index) {
        const auto transaction_name =
            fixture.at("transactions")[index].at("name").get<std::string>();
        const auto found = clients_by_cdc_transaction.find(transaction_name);
        if (found == clients_by_cdc_transaction.end()) {
          throw std::runtime_error("captured TPC-C join mapping is incomplete");
        }
        const bool drop_client = mutation == Mutation::DropClient &&
                                 transaction_name == "insert_then_delete";
        const bool drop_cdc = mutation == Mutation::DropCdc &&
                              transaction_name == "primary_key_change";
        if (!drop_client) {
          controller.acceptClientPayload(found->second.dump());
        }
        if (drop_cdc) {
          cdc_dropped = true;
        } else {
          if (cdc_dropped) {
            --transactions[index].cdc_seq;
          }
          controller.acceptCdcTransaction(std::move(transactions[index]));
        }
      }
      if (cdc_dropped) {
        --transactions[11].cdc_seq;
      }
    }
    controller.acceptClientPayload(client_fixture.at("session_done").dump());
    controller.acceptCdcTransaction(std::move(transactions[11]));
    controller.onInputIdle();

    if (!controller.terminal() || !controller.finalOutcome().has_value() ||
        !controller.finalReconciliation().has_value()) {
      throw std::runtime_error("captured TPC-C run did not finalize");
    }
    return Result{.outcome = *controller.finalOutcome(),
                  .reconciliation = *controller.finalReconciliation(),
                  .metrics = controller.joinerMetrics(),
                  .diagnostics = controller.diagnostics()};
  };

  const auto has_code = [](const Result &result, const std::string_view code) {
    return std::any_of(
        result.diagnostics.begin(), result.diagnostics.end(),
        [&](const auto &diagnostic) { return diagnostic.code == code; });
  };
  const auto positive = run(Mutation::None);
  BLACKBOX_CHECK(positive.outcome == FinalOutcome::Pass);
  BLACKBOX_CHECK(positive.reconciliation.evidence_reconciled);
  BLACKBOX_CHECK(positive.reconciliation.bootstrap_manifest_validated);
  BLACKBOX_CHECK(positive.reconciliation.end_fence_seen);
  BLACKBOX_CHECK(positive.reconciliation.expected_sessions == 1U);
  BLACKBOX_CHECK(positive.reconciliation.completed_sessions == 1U);
  BLACKBOX_CHECK(positive.reconciliation.accepted_client_transactions == 8U);
  BLACKBOX_CHECK(positive.reconciliation.joined_workload_transactions == 8U);
  BLACKBOX_CHECK(positive.reconciliation.unmatched_client_records == 0U);
  BLACKBOX_CHECK(positive.reconciliation.unmatched_workload_cdc_slots == 0U);
  BLACKBOX_CHECK(positive.reconciliation.ambiguous_attempts == 0U);
  BLACKBOX_CHECK(positive.metrics.marker_only_workload_transactions == 2U);
  BLACKBOX_CHECK(positive.metrics.joined_read_events == 13U);
  BLACKBOX_CHECK(positive.metrics.joined_write_events == 18U);

  const auto dropped_client = run(Mutation::DropClient);
  BLACKBOX_CHECK(dropped_client.outcome == FinalOutcome::Invalid);
  BLACKBOX_CHECK(!dropped_client.reconciliation.evidence_reconciled);
  BLACKBOX_CHECK(dropped_client.reconciliation.unmatched_workload_cdc_slots >
                 0U);

  const auto dropped_cdc = run(Mutation::DropCdc);
  BLACKBOX_CHECK(dropped_cdc.outcome == FinalOutcome::Invalid);
  BLACKBOX_CHECK(!dropped_cdc.reconciliation.evidence_reconciled);
  BLACKBOX_CHECK(dropped_cdc.reconciliation.unmatched_client_records > 0U);

  const auto wrong_typed = run(Mutation::WrongTypedValue);
  BLACKBOX_CHECK(wrong_typed.outcome == FinalOutcome::Invalid);
  BLACKBOX_CHECK(!wrong_typed.reconciliation.evidence_reconciled);
  BLACKBOX_CHECK(has_code(wrong_typed, "CANONICAL_ENCODING_FAILED"));

  const auto exhausted = run(Mutation::ResourceExhaustion);
  BLACKBOX_CHECK(exhausted.outcome == FinalOutcome::Invalid);
  BLACKBOX_CHECK(!exhausted.reconciliation.evidence_reconciled);
  BLACKBOX_CHECK(has_code(exhausted, "JOIN_BUFFER_LIMIT_EXCEEDED"));
}

void testTpccStandardMixSessionAccounting() {
  constexpr std::array<std::string_view, 5> transaction_types = {
      "NewOrder", "Payment", "OrderStatus", "Delivery", "StockLevel"};
  constexpr std::array<unsigned int, 5> standard_weights = {45U, 43U, 4U, 4U,
                                                            4U};
  BLACKBOX_CHECK(std::accumulate(standard_weights.begin(),
                                 standard_weights.end(), 0U) == 100U);

  auto manifest = tpccManifest();
  manifest.expected_session_ids = {"tpcc/worker/1", "tpcc/worker/2",
                                   "tpcc/worker/3", "tpcc/worker/4"};
  RunController controller(manifest);
  openTpccWorkload(controller);

  const std::vector<ClientRead> no_reads;
  const auto client_record = [&](const std::string &session,
                                 const std::uint64_t session_seq,
                                 const std::uint64_t txn_id,
                                 const std::string_view txn_type,
                                 const std::uint64_t unique) {
    return Json{
        {"kind", "client_txn"},
        {"run_id", run_id},
        {"session_id", session},
        {"session_seq", session_seq},
        {"logical_txn_id", uuid(unique, '3')},
        {"attempt_id", uuid(unique, '4')},
        {"txn_type", txn_type},
        {"correlation_kind", "session_txn"},
        {"correlation_id",
         Json{{"run_id", run_id}, {"session_id", session}, {"txn_id", txn_id}}},
        {"read_count", 0},
        {"read_digest", blackbox::computeClientReadDigest(no_reads)},
        {"reads", Json::array()},
    };
  };
  const auto marker_only = [&](const std::uint64_t cdc_seq,
                               const std::string &session,
                               const std::uint64_t txn_id) {
    return CdcTransaction{
        .cdc_seq = cdc_seq,
        .kind = CdcTransactionKind::WorkloadMarked,
        .fence_phase = std::nullopt,
        .correlation_id = RunScopedCorrelationId{.run_id = std::string(run_id),
                                                 .session_id = session,
                                                 .txn_id = txn_id},
        .writes = {},
        .begin_stream_id = std::to_string(cdc_seq + 10U) + "-0",
        .end_stream_id = std::to_string(cdc_seq + 10U) + "-2",
    };
  };

  std::map<std::string, std::size_t> published_per_type;
  std::uint64_t cdc_seq = 2U;
  for (std::size_t session_index = 0U;
       session_index < manifest.expected_session_ids.size(); ++session_index) {
    const auto &session = manifest.expected_session_ids[session_index];
    std::vector<SessionMarkerEntry> published;
    for (std::size_t type_index = 0U; type_index < transaction_types.size();
         ++type_index) {
      const auto sequence = static_cast<std::uint64_t>(type_index + 1U);
      const auto unique =
          static_cast<std::uint64_t>((session_index + 1U) * 100U) + sequence;
      auto client = client_record(session, sequence, sequence,
                                  transaction_types[type_index], unique);
      auto cdc = marker_only(cdc_seq, session, sequence);
      if (cdc_seq % 2U == 0U) {
        controller.acceptClientPayload(client.dump());
        controller.acceptCdcTransaction(std::move(cdc));
      } else {
        controller.acceptCdcTransaction(std::move(cdc));
        controller.acceptClientPayload(client.dump());
      }
      published.push_back(
          SessionMarkerEntry{.session_seq = sequence, .txn_id = sequence});
      ++published_per_type[std::string(transaction_types[type_index])];
      ++cdc_seq;
    }

    if (session_index == 0U) {
      constexpr std::uint64_t delivery_sequence = 6U;
      constexpr std::uint64_t committed_attempt = 7U;
      auto client = client_record(session, delivery_sequence, committed_attempt,
                                  "Delivery", 106U);
      controller.acceptCdcTransaction(
          marker_only(cdc_seq, session, committed_attempt));
      controller.acceptClientPayload(client.dump());
      published.push_back(SessionMarkerEntry{.session_seq = delivery_sequence,
                                             .txn_id = committed_attempt});
      ++published_per_type["Delivery"];
      ++cdc_seq;
    }

    const bool first_session = session_index == 0U;
    const auto logical = first_session ? 7U : 5U;
    const auto physical = first_session ? 8U : 5U;
    const auto successful = first_session ? 6U : 5U;
    controller.acceptClientPayload(Json{
        {"kind", "session_done"},
        {"run_id", run_id},
        {"session_id", session},
        {"final_session_seq", logical},
        {"logical_transactions_started", logical},
        {"successful_transactions", successful},
        {"terminal_aborted_transactions", first_session ? 1U : 0U},
        {"physical_attempts_started", physical},
        {"definitively_aborted_attempts", first_session ? 2U : 0U},
        {"published_transactions", successful},
        {"retry_attempts", first_session ? 1U : 0U},
        {"ambiguous_attempts", 0},
        {"rolling_correlation_digest",
         blackbox::computeRollingSessionMarkerDigest(
             run_id, session,
             published)}}.dump());
  }

  controller.acceptCdcTransaction(control(cdc_seq, RunFencePhase::EndFence));
  controller.onInputIdle();
  BLACKBOX_CHECK(controller.terminal());
  BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Pass);
  BLACKBOX_REQUIRE(controller.finalReconciliation().has_value());
  const auto &reconciliation = *controller.finalReconciliation();
  BLACKBOX_CHECK(reconciliation.evidence_reconciled);
  BLACKBOX_CHECK(reconciliation.expected_sessions == 4U);
  BLACKBOX_CHECK(reconciliation.completed_sessions == 4U);
  BLACKBOX_CHECK(reconciliation.accepted_client_transactions == 21U);
  BLACKBOX_CHECK(reconciliation.joined_workload_transactions == 21U);
  BLACKBOX_CHECK(reconciliation.unmatched_client_records == 0U);
  BLACKBOX_CHECK(reconciliation.unmatched_workload_cdc_slots == 0U);
  BLACKBOX_CHECK(reconciliation.ambiguous_attempts == 0U);
  const std::map<std::string, std::size_t> expected_standard_type_counts = {
      {"Delivery", 5U},
      {"NewOrder", 4U},
      {"OrderStatus", 4U},
      {"Payment", 4U},
      {"StockLevel", 4U}};
  BLACKBOX_CHECK(published_per_type == expected_standard_type_counts);
  const auto metrics = controller.joinerMetrics();
  BLACKBOX_CHECK(metrics.joined_workload_transactions == 21U);
  BLACKBOX_CHECK(metrics.marker_only_workload_transactions == 21U);
  BLACKBOX_CHECK(metrics.joined_read_events == 0U);
  BLACKBOX_CHECK(metrics.joined_write_events == 0U);
  BLACKBOX_CHECK(metrics.client_first_matches > 0U);
  BLACKBOX_CHECK(metrics.cdc_first_matches > 0U);
}

void testCapturedTpccControllerReconciliation() {
  const auto fixture = loadTpccTransactionFixture();
  const auto client_fixture = loadTpccClientFixture();
  BLACKBOX_CHECK(client_fixture.at("raw_transaction_fixture").at("path") ==
                 "protocol/fixtures/mariadb_tpcc_transactions.json");
  BLACKBOX_CHECK(client_fixture.at("run_manifest").at("run_id") ==
                 fixture.at("run_id"));
  auto transactions = assembleTpccFixture(fixture);
  BLACKBOX_REQUIRE(transactions.size() == 12U);
  const auto new_order_client = std::find_if(
      client_fixture.at("semantic_transactions").begin(),
      client_fixture.at("semantic_transactions").end(), [](const Json &entry) {
        return entry.at("client_txn").at("txn_type") == "NewOrder";
      });
  BLACKBOX_REQUIRE(new_order_client !=
                   client_fixture.at("semantic_transactions").end());
  BLACKBOX_CHECK(new_order_client->at("cdc_transaction") == "new_order_shape");
  BLACKBOX_CHECK(new_order_client->at("client_txn").at("read_count") == 3U);
  BLACKBOX_CHECK(
      new_order_client->at("client_txn").at("correlation_id").at("txn_id") ==
      2U);
  const auto new_order_cdc = std::find_if(
      fixture.at("transactions").begin(), fixture.at("transactions").end(),
      [](const Json &entry) { return entry.at("name") == "new_order_shape"; });
  BLACKBOX_REQUIRE(new_order_cdc != fixture.at("transactions").end());
  BLACKBOX_CHECK(new_order_cdc->at("expected").at("writes").size() == 6U);
  RunController controller(blackbox::ProtocolRecordParser{}.parseRunManifest(
      client_fixture.at("run_manifest").dump()));
  discardStatuses(controller);
  controller.acceptCdcTransaction(std::move(transactions[0]));
  controller.acceptCdcTransaction(std::move(transactions[1]));
  controller.acceptClientPayload(fixture.at("bootstrap_manifest").dump());
  controller.acceptCdcTransaction(std::move(transactions[2]));
  BLACKBOX_CHECK(controller.joinerMetrics().bootstrap_state_validated);
  std::map<std::string, Json> clients_by_cdc_transaction;
  for (const auto field : {"semantic_transactions", "synthetic_join_clients"}) {
    for (const auto &entry : client_fixture.at(field)) {
      clients_by_cdc_transaction.emplace(
          entry.at("cdc_transaction").get<std::string>(),
          entry.at("client_txn"));
    }
  }
  BLACKBOX_REQUIRE(clients_by_cdc_transaction.size() == 8U);
  for (std::size_t index = 0; index < 8U; ++index) {
    const auto transaction_name =
        fixture.at("transactions")[index + 3U].at("name").get<std::string>();
    const auto found = clients_by_cdc_transaction.find(transaction_name);
    BLACKBOX_REQUIRE(found != clients_by_cdc_transaction.end());
    auto client = found->second.dump();
    if (index % 2U == 0U) {
      controller.acceptClientPayload(client);
      controller.acceptCdcTransaction(std::move(transactions[index + 3U]));
    } else {
      controller.acceptCdcTransaction(std::move(transactions[index + 3U]));
      controller.acceptClientPayload(client);
    }
  }
  controller.acceptClientPayload(client_fixture.at("session_done").dump());
  controller.acceptCdcTransaction(std::move(transactions[11]));
  controller.onInputIdle();

  BLACKBOX_CHECK(controller.terminal());
  BLACKBOX_CHECK(controller.finalOutcome() == FinalOutcome::Pass);
  BLACKBOX_REQUIRE(controller.finalReconciliation().has_value());
  const auto &reconciliation = *controller.finalReconciliation();
  BLACKBOX_CHECK(reconciliation.evidence_reconciled);
  BLACKBOX_CHECK(reconciliation.bootstrap_manifest_validated);
  BLACKBOX_CHECK(reconciliation.end_fence_seen);
  BLACKBOX_CHECK(reconciliation.expected_sessions == 1U);
  BLACKBOX_CHECK(reconciliation.completed_sessions == 1U);
  BLACKBOX_CHECK(reconciliation.accepted_client_transactions == 8U);
  BLACKBOX_CHECK(reconciliation.joined_workload_transactions == 8U);
  BLACKBOX_CHECK(reconciliation.bootstrap_cdc_transactions == 1U);
  BLACKBOX_CHECK(reconciliation.bootstrap_checker_transactions == 1U);
  BLACKBOX_CHECK(reconciliation.phase_anchor_transactions == 1U);
  BLACKBOX_CHECK(reconciliation.unmatched_client_records == 0U);
  BLACKBOX_CHECK(reconciliation.unmatched_workload_cdc_slots == 0U);
  const auto metrics = controller.joinerMetrics();
  BLACKBOX_CHECK(metrics.accepted_cdc_transactions == 12U);
  BLACKBOX_CHECK(metrics.joined_workload_transactions == 8U);
  BLACKBOX_CHECK(metrics.marker_only_workload_transactions == 2U);
  BLACKBOX_CHECK(metrics.client_first_matches == 4U);
  BLACKBOX_CHECK(metrics.cdc_first_matches == 4U);
  BLACKBOX_CHECK(metrics.joined_read_events == 13U);
  BLACKBOX_CHECK(metrics.joined_write_events == 18U);
  BLACKBOX_CHECK(metrics.released_bootstrap_transactions == 1U);
  BLACKBOX_CHECK(metrics.released_phase_anchor_transactions == 1U);
  BLACKBOX_CHECK(metrics.unmatched_client_records == 0U);
  BLACKBOX_CHECK(metrics.unmatched_workload_cdc_slots == 0U);
}

} // namespace

int runRunControllerTests() {
  testCleanPassRequiresDoneEndAndIdle();
  testEndWithoutDoneIsInvalid();
  testOtherwiseCleanRunWithoutEndFenceIsInvalid();
  testTimeoutAndDuplicateControlRecordsAreInvalid();
  testIncorrectInitialBootstrapManifestPreventsRelease();
  testMissingClientDoesNotProcessEndFence();
  testMissingCdcIsInvalidAfterOrderedEnd();
  testSessionDoneMismatchAndPostDoneClientAreInvalid();
  testStoppedAdmissionDoesNotInventCdcOrderFailure();
  testSessionDoneRequiresFeasibleFinalAttemptHistory();
  testViolationThenOperationalInvalidityPreservesObservation();
  testJoinLimitAfterViolationPreservesObservation();
  testCleanViolationFinalizesAsViolation();
  testFirstViolationOwnsReservedDiagnosticSlotImmediately();
  testJoinOnlyReconcilesWouldBeViolationWithoutVerdict();
  testJoinOnlyFailureHasNoAuditedOutcome();
  testCompletedSessionTrackingUsesTerminalAudit();
  testMonitorOwnedCapabilitiesAdmitTpcc();
  testAdapterManifestSemanticsAreValidatedAtAdmission();
  testTpccControllerUsesGenericJoinAndReconciliation();
  testCapturedPaymentRetryReconciliation();
  testCapturedDeliveryRetryReconciliation();
  testDeliveryEmptyAndTerminalFailureAlgebra();
  testStockLevelFocusedReconciliationAndMalformedEvidence();
  testCapturedTpccFullMixAndStructuralFailures();
  testTpccStandardMixSessionAccounting();
  testCapturedTpccControllerReconciliation();
  return 0;
}
