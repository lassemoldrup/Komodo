#include "blackbox/transaction_joiner.h"

#include "blackbox/canonical_ycsb_codec.h"
#include "test_support.h"

#include <nlohmann/json.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iomanip>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using blackbox::BootstrapPrefix;
using blackbox::BootstrapStateProof;
using blackbox::BootstrapTableCount;
using blackbox::CanonicalBytes;
using blackbox::CanonicalYcsbCodec;
using blackbox::CdcTransaction;
using blackbox::CdcTransactionKind;
using blackbox::CheckerObservation;
using blackbox::ClientRead;
using blackbox::ClientReadSource;
using blackbox::ClientValueState;
using blackbox::HeadBlockReason;
using blackbox::JoinLifecycle;
using blackbox::MonitorError;
using blackbox::MonitorErrorCode;
using blackbox::NormalizedValueState;
using blackbox::NormalizedWrite;
using blackbox::RunFencePhase;
using blackbox::RunScopedCorrelationId;
using blackbox::TransactionJoiner;
using blackbox::TransactionJoinerConfig;
using blackbox::TransactionJoinerLimits;
using Json = nlohmann::json;

constexpr std::string_view run_id = "00112233-4455-6677-8899-aabbccddeeff";
constexpr std::string_view session_one = "ycsb/worker/1";
constexpr std::string_view session_two = "ycsb/worker/2";

[[nodiscard]] TransactionJoinerConfig
config(const TransactionJoinerLimits limits = {}) {
  return TransactionJoinerConfig{
      .run_id = std::string(run_id),
      .expected_session_ids = {std::string(session_one),
                               std::string(session_two)},
      .application_tables = {"benchbase.usertable"},
      .benchmark_adapter = &blackbox::BenchmarkAdapterRegistry::require("ycsb"),
      .client_parser_limits = {},
      .limits = limits,
  };
}

[[nodiscard]] CanonicalBytes key(const std::int32_t value) {
  return CanonicalYcsbCodec::encodeKey("benchbase", "usertable", value);
}

[[nodiscard]] CanonicalBytes value(const std::string &field_one) {
  std::array<std::optional<std::string>, 10> fields{};
  fields[0] = field_one;
  return CanonicalYcsbCodec::encodeValue(fields);
}

[[nodiscard]] CanonicalBytes allNullValue() {
  std::array<std::optional<std::string>, 10> fields{};
  return CanonicalYcsbCodec::encodeValue(fields);
}

[[nodiscard]] CanonicalBytes mixedGoldenValue() {
  const std::array<std::optional<std::string>, 10> fields = {
      std::nullopt, "",   "A",
      "é",          "中", "0",
      "hello",      "🙂", std::string("a\0b", 3U),
      "last"};
  return CanonicalYcsbCodec::encodeValue(fields);
}

[[nodiscard]] NormalizedWrite presentWrite(const std::int32_t key_value,
                                           const std::string &field_one) {
  return NormalizedWrite{.key = key(key_value),
                         .state = NormalizedValueState::Present,
                         .value = value(field_one)};
}

[[nodiscard]] ClientRead presentReadBytes(const std::uint32_t sequence,
                                          const std::int32_t key_value,
                                          CanonicalBytes bytes) {
  return ClientRead{.event_seq = sequence,
                    .key = key(key_value),
                    .state = ClientValueState::Present,
                    .value = std::move(bytes),
                    .source = ClientReadSource::Snapshot};
}

[[nodiscard]] ClientRead absentRead(const std::uint32_t sequence,
                                    const std::int32_t key_value) {
  return ClientRead{.event_seq = sequence,
                    .key = key(key_value),
                    .state = ClientValueState::Absent,
                    .value = std::nullopt,
                    .source = ClientReadSource::Snapshot};
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

[[nodiscard]] std::string clientPayload(
    const std::string_view session_id, const std::uint64_t session_seq,
    const std::uint64_t txn_id, const std::uint64_t identity_sequence,
    const std::vector<ClientRead> &reads = {},
    const std::optional<std::string> &logical_override = std::nullopt,
    const std::optional<std::string> &attempt_override = std::nullopt) {
  Json read_array = Json::array();
  for (const auto &read : reads) {
    read_array.push_back(readJson(read));
  }
  return Json{
      {"kind", "client_txn"},
      {"run_id", run_id},
      {"session_id", session_id},
      {"session_seq", session_seq},
      {"logical_txn_id",
       logical_override.value_or(uuid(identity_sequence, '1'))},
      {"attempt_id", attempt_override.value_or(uuid(identity_sequence, '2'))},
      {"txn_type", "ycsb_update"},
      {"correlation_kind", "session_txn"},
      {"correlation_id", Json{{"run_id", run_id},
                              {"session_id", session_id},
                              {"txn_id", txn_id}}},
      {"read_count", reads.size()},
      {"read_digest", blackbox::computeClientReadDigest(reads)},
      {"reads", std::move(read_array)},
  }
      .dump();
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

[[nodiscard]] CdcTransaction bootstrap(const std::uint64_t cdc_seq,
                                       std::vector<NormalizedWrite> writes) {
  return CdcTransaction{
      .cdc_seq = cdc_seq,
      .kind = CdcTransactionKind::BootstrapApp,
      .fence_phase = std::nullopt,
      .correlation_id = std::nullopt,
      .writes = std::move(writes),
      .begin_stream_id = std::to_string(cdc_seq + 1U) + "-0",
      .end_stream_id = std::to_string(cdc_seq + 1U) + "-2",
  };
}

[[nodiscard]] CdcTransaction
workload(const std::uint64_t cdc_seq, const std::string_view session_id,
         const std::uint64_t txn_id, std::vector<NormalizedWrite> writes = {}) {
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

[[nodiscard]] BootstrapStateProof
emptyBootstrapProof(TransactionJoiner &joiner) {
  BLACKBOX_CHECK(joiner.pendingBootstrapPrefix().has_value());
  return BootstrapStateProof{
      .prefix = joiner.pendingBootstrapPrefix().value(),
      .table_row_counts = {BootstrapTableCount{.table = "benchbase.usertable",
                                               .row_count = 0}},
      .canonical_state_digest =
          "944dd6136dbad7047e31af3b6419eb6a632d4dea64bec8159e93307163810bc2",
  };
}

void approveLoad(TransactionJoiner &joiner) {
  joiner.validateBootstrapState(emptyBootstrapProof(joiner));
}

void openWorkload(TransactionJoiner &joiner) {
  joiner.acceptCdcTransaction(control(0U, RunFencePhase::RunStart));
  joiner.acceptCdcTransaction(control(1U, RunFencePhase::LoadFinished));
  BLACKBOX_CHECK(joiner.headBlockReason() ==
                 HeadBlockReason::BootstrapValidation);
  approveLoad(joiner);
  BLACKBOX_CHECK(joiner.lifecycle() == JoinLifecycle::Workload);
}

void expectError(TransactionJoiner &joiner, const std::function<void()> &action,
                 const MonitorErrorCode expected) {
  try {
    action();
    BLACKBOX_CHECK(false);
  } catch (const MonitorError &error) {
    BLACKBOX_CHECK(error.code() == expected);
    BLACKBOX_CHECK(joiner.failed());
    BLACKBOX_CHECK(joiner.errorCode() == expected);
  }
}

void testStrictReadyPrefixAndEndDeferral() {
  TransactionJoiner joiner(config());
  openWorkload(joiner);

  joiner.acceptClientPayload(clientPayload(session_two, 1U, 1U, 2U));
  joiner.acceptCdcTransaction(workload(2U, session_one, 1U));
  joiner.acceptCdcTransaction(workload(3U, session_two, 1U));
  auto metrics = joiner.metrics();
  BLACKBOX_CHECK(metrics.joined_workload_transactions == 0U);
  BLACKBOX_CHECK(metrics.pending_cdc_slots == 2U);
  BLACKBOX_CHECK(metrics.pending_client_records == 1U);
  BLACKBOX_CHECK(metrics.unmatched_client_records == 0U);
  BLACKBOX_CHECK(metrics.unmatched_workload_cdc_slots == 1U);
  BLACKBOX_CHECK(metrics.client_first_matches == 1U);
  BLACKBOX_CHECK(joiner.headBlockReason() == HeadBlockReason::MissingClient);
  joiner.acceptCdcTransaction(control(4U, RunFencePhase::EndFence));
  metrics = joiner.metrics();
  BLACKBOX_CHECK(metrics.end_fence_observed);
  BLACKBOX_CHECK(metrics.pending_cdc_slots == 3U);
  BLACKBOX_CHECK(joiner.headBlockReason() == HeadBlockReason::MissingClient);

  joiner.acceptClientPayload(clientPayload(session_one, 1U, 1U, 1U));
  metrics = joiner.metrics();
  BLACKBOX_CHECK(metrics.joined_workload_transactions == 2U);
  BLACKBOX_CHECK(metrics.pending_cdc_slots == 1U);
  BLACKBOX_CHECK(metrics.pending_client_records == 0U);
  BLACKBOX_CHECK(metrics.cdc_first_matches == 1U);
  BLACKBOX_CHECK(metrics.checker.next_commit_seq == 4U);
  BLACKBOX_CHECK(metrics.checker.phase_anchor_transactions == 2U);
  BLACKBOX_CHECK(metrics.checker.workload_transactions == 2U);
  BLACKBOX_CHECK(metrics.marker_only_workload_transactions == 2U);

  metrics = joiner.metrics();
  BLACKBOX_CHECK(metrics.checker.next_commit_seq == 4U);
  BLACKBOX_CHECK(joiner.headBlockReason() == HeadBlockReason::EndFenceDeferred);
  joiner.drain();
  BLACKBOX_CHECK(joiner.metrics().pending_cdc_slots == 1U);
  joiner.processEndFence();
  metrics = joiner.metrics();
  BLACKBOX_CHECK(joiner.lifecycle() == JoinLifecycle::Closed);
  BLACKBOX_CHECK(metrics.pending_cdc_slots == 0U);
  BLACKBOX_CHECK(metrics.end_fence_processed);
  BLACKBOX_CHECK(metrics.bootstrap_state_validated);
  BLACKBOX_CHECK(metrics.accepted_control_cdc_transactions == 3U);
  BLACKBOX_CHECK(metrics.accepted_bootstrap_cdc_transactions == 0U);
  BLACKBOX_CHECK(metrics.accepted_workload_cdc_transactions == 2U);
  BLACKBOX_CHECK(metrics.unmatched_client_records == 0U);
  BLACKBOX_CHECK(metrics.unmatched_workload_cdc_slots == 0U);
  const auto audit = joiner.auditReconciliationState();
  BLACKBOX_CHECK(audit.incremental_counters_match);
  BLACKBOX_CHECK(audit.pending_client_records == 0U);
  BLACKBOX_CHECK(audit.pending_cdc_slots == 0U);
  BLACKBOX_CHECK(audit.unmatched_client_records == 0U);
  BLACKBOX_CHECK(audit.unmatched_workload_cdc_slots == 0U);
}

void testHeadWaitClockAndCombinedJoinBufferPeak() {
  std::uint64_t now = 100U;
  auto joiner_config = config();
  joiner_config.monotonic_clock_nanoseconds = [&] { return now; };
  TransactionJoiner joiner(std::move(joiner_config));
  openWorkload(joiner);

  joiner.acceptCdcTransaction(workload(2U, session_one, 1U));
  now = 140U;
  joiner.acceptClientPayload(clientPayload(session_two, 1U, 1U, 2U));
  now = 170U;
  joiner.acceptCdcTransaction(workload(3U, session_two, 1U));
  now = 190U;
  auto metrics = joiner.metrics();
  BLACKBOX_CHECK(metrics.missing_client_head_wait_count == 1U);
  BLACKBOX_CHECK(metrics.missing_client_head_wait_current_nanoseconds == 90U);
  BLACKBOX_CHECK(metrics.missing_client_head_wait_total_nanoseconds == 90U);
  BLACKBOX_CHECK(metrics.missing_client_head_wait_max_nanoseconds == 90U);
  BLACKBOX_CHECK(metrics.join_buffer_high_water_bytes ==
                 metrics.pending_client_bytes + metrics.pending_cdc_bytes);
  BLACKBOX_CHECK(metrics.join_buffer_high_water_bytes >
                 metrics.pending_client_high_water_bytes);
  BLACKBOX_CHECK(metrics.join_buffer_high_water_bytes >
                 metrics.pending_cdc_high_water_bytes);

  now = 250U;
  joiner.acceptClientPayload(clientPayload(session_one, 1U, 1U, 1U));
  metrics = joiner.metrics();
  BLACKBOX_CHECK(metrics.joined_workload_transactions == 2U);
  BLACKBOX_CHECK(metrics.missing_client_head_wait_count == 1U);
  BLACKBOX_CHECK(metrics.missing_client_head_wait_current_nanoseconds == 0U);
  BLACKBOX_CHECK(metrics.missing_client_head_wait_total_nanoseconds == 150U);
  BLACKBOX_CHECK(metrics.missing_client_head_wait_max_nanoseconds == 150U);

  now = 300U;
  joiner.acceptCdcTransaction(workload(4U, session_one, 2U));
  now = 320U;
  joiner.drain();
  now = 340U;
  joiner.acceptClientPayload(clientPayload(session_one, 2U, 2U, 3U));
  metrics = joiner.metrics();
  BLACKBOX_CHECK(metrics.joined_workload_transactions == 3U);
  BLACKBOX_CHECK(metrics.missing_client_head_wait_count == 2U);
  BLACKBOX_CHECK(metrics.missing_client_head_wait_current_nanoseconds == 0U);
  BLACKBOX_CHECK(metrics.missing_client_head_wait_total_nanoseconds == 190U);
  BLACKBOX_CHECK(metrics.missing_client_head_wait_max_nanoseconds == 150U);
}

void testOrderedJoiningWithoutCheckerExecution() {
  auto joiner_config = config();
  joiner_config.execute_consistency_check = false;
  TransactionJoiner joiner(std::move(joiner_config));

  joiner.acceptCdcTransaction(control(0U, RunFencePhase::RunStart));
  joiner.acceptCdcTransaction(
      bootstrap(1U, {NormalizedWrite{.key = key(42),
                                     .state = NormalizedValueState::Present,
                                     .value = mixedGoldenValue()},
                     NormalizedWrite{.key = key(-1),
                                     .state = NormalizedValueState::Present,
                                     .value = allNullValue()}}));
  joiner.acceptCdcTransaction(control(2U, RunFencePhase::LoadFinished));
  const auto prefix = joiner.pendingBootstrapPrefix();
  BLACKBOX_REQUIRE(prefix.has_value());
  joiner.validateBootstrapState(BootstrapStateProof{
      .prefix = *prefix,
      .table_row_counts = {BootstrapTableCount{.table = "benchbase.usertable",
                                               .row_count = 2}},
      .canonical_state_digest =
          "71749720e062cf361dbad1e139c88d7d39b10debeddf18562d35cd80ae53afa8",
  });

  joiner.acceptClientPayload(
      clientPayload(session_one, 1U, 1U, 1U, {absentRead(0U, 42)}));
  joiner.acceptCdcTransaction(workload(3U, session_one, 1U));
  joiner.acceptClientPayload(clientPayload(session_two, 1U, 1U, 2U));
  joiner.acceptCdcTransaction(
      workload(4U, session_two, 1U, {presentWrite(7, "written")}));
  joiner.acceptCdcTransaction(control(5U, RunFencePhase::EndFence));
  joiner.processEndFence();

  const auto metrics = joiner.metrics();
  BLACKBOX_CHECK(joiner.lifecycle() == JoinLifecycle::Closed);
  BLACKBOX_CHECK(metrics.bootstrap_state_validated);
  BLACKBOX_CHECK(metrics.released_bootstrap_transactions == 1U);
  BLACKBOX_CHECK(metrics.released_phase_anchor_transactions == 2U);
  BLACKBOX_CHECK(metrics.released_workload_transactions == 2U);
  BLACKBOX_CHECK(metrics.released_bootstrap_write_events == 2U);
  BLACKBOX_CHECK(metrics.released_ordered_events == 4U);
  BLACKBOX_CHECK(metrics.joined_workload_transactions == 2U);
  BLACKBOX_CHECK(metrics.marker_only_workload_transactions == 1U);
  BLACKBOX_CHECK(metrics.joined_read_events == 1U);
  BLACKBOX_CHECK(metrics.joined_write_events == 1U);
  BLACKBOX_CHECK(metrics.joined_ordered_events == 2U);
  BLACKBOX_CHECK(joiner.checkerObservation() == CheckerObservation::NotStarted);
  BLACKBOX_CHECK(!joiner.firstViolation().has_value());
  BLACKBOX_CHECK(metrics.checker.next_commit_seq == 0U);
  BLACKBOX_CHECK(metrics.checker.consumed_transactions == 0U);
  BLACKBOX_CHECK(metrics.checker.bootstrap_transactions == 0U);
  BLACKBOX_CHECK(metrics.checker.phase_anchor_transactions == 0U);
  BLACKBOX_CHECK(metrics.checker.workload_transactions == 0U);
  BLACKBOX_CHECK(metrics.checker.read_events == 0U);
  BLACKBOX_CHECK(metrics.checker.write_events == 0U);
  BLACKBOX_CHECK(metrics.checker.consume_total_nanoseconds == 0U);
  BLACKBOX_CHECK(metrics.checker.consume_max_nanoseconds == 0U);
  BLACKBOX_CHECK(metrics.checker.mapped_identity_records == 0U);
  BLACKBOX_CHECK(metrics.checker.mapped_identity_bytes == 0U);
  BLACKBOX_CHECK(metrics.checker.engine.retained_state_capacity_bytes == 0U);
  BLACKBOX_CHECK(metrics.checker.engine.retained_state_bytes == 0U);
}

void testEndFenceCannotPassMissingClient() {
  TransactionJoiner joiner(config());
  openWorkload(joiner);
  joiner.acceptCdcTransaction(workload(2U, session_one, 1U));
  joiner.acceptCdcTransaction(control(3U, RunFencePhase::EndFence));
  auto metrics = joiner.metrics();
  BLACKBOX_CHECK(metrics.end_fence_observed);
  BLACKBOX_CHECK(!metrics.end_fence_processed);
  BLACKBOX_CHECK(metrics.unmatched_workload_cdc_slots == 1U);
  expectError(
      joiner, [&] { joiner.processEndFence(); },
      MonitorErrorCode::FenceOrderInvalid);
  metrics = joiner.metrics();
  BLACKBOX_CHECK(!metrics.end_fence_processed);
  BLACKBOX_CHECK(metrics.pending_cdc_slots == 2U);
}

void testBootstrapGateAndAnchorLowerBound() {
  TransactionJoiner joiner(config());
  joiner.acceptCdcTransaction(control(0U, RunFencePhase::RunStart));
  joiner.acceptCdcTransaction(
      bootstrap(1U, {NormalizedWrite{.key = key(42),
                                     .state = NormalizedValueState::Present,
                                     .value = mixedGoldenValue()},
                     NormalizedWrite{.key = key(-1),
                                     .state = NormalizedValueState::Present,
                                     .value = allNullValue()}}));
  joiner.acceptCdcTransaction(control(2U, RunFencePhase::LoadFinished));
  const auto pending = joiner.pendingBootstrapPrefix();
  BLACKBOX_REQUIRE(pending.has_value());
  BLACKBOX_CHECK(pending->run_id == run_id);
  BLACKBOX_CHECK(pending->load_fence_cdc_seq == 2U);
  BLACKBOX_CHECK(pending->bootstrap_transactions == 1U);
  BLACKBOX_CHECK(joiner.metrics().checker.bootstrap_transactions == 1U);
  BLACKBOX_CHECK(joiner.metrics().released_bootstrap_write_events == 2U);
  BLACKBOX_CHECK(joiner.metrics().released_ordered_events == 2U);
  BLACKBOX_CHECK(joiner.metrics().checker.phase_anchor_transactions == 0U);
  joiner.acceptClientPayload(clientPayload(
      session_two, 1U, 1U, 2U, {presentReadBytes(0U, 42, mixedGoldenValue())}));
  joiner.acceptCdcTransaction(workload(3U, session_two, 1U));
  BLACKBOX_CHECK(joiner.metrics().joined_workload_transactions == 0U);
  BLACKBOX_CHECK(joiner.metrics().pending_cdc_slots == 2U);
  BLACKBOX_CHECK(joiner.headBlockReason() ==
                 HeadBlockReason::BootstrapValidation);
  joiner.validateBootstrapState(BootstrapStateProof{
      .prefix = *pending,
      .table_row_counts = {BootstrapTableCount{.table = "benchbase.usertable",
                                               .row_count = 2}},
      .canonical_state_digest =
          "71749720e062cf361dbad1e139c88d7d39b10debeddf18562d35cd80ae53afa8",
  });
  BLACKBOX_CHECK(joiner.metrics().checker.phase_anchor_transactions == 2U);
  BLACKBOX_CHECK(joiner.metrics().joined_workload_transactions == 1U);
  BLACKBOX_CHECK(joiner.metrics().released_ordered_events == 3U);
  BLACKBOX_CHECK(joiner.checkerObservation() == CheckerObservation::PassSoFar);

  joiner.acceptClientPayload(
      clientPayload(session_one, 1U, 1U, 1U, {absentRead(0U, 42)}));
  joiner.acceptCdcTransaction(workload(4U, session_one, 1U));
  BLACKBOX_CHECK(joiner.checkerObservation() ==
                 CheckerObservation::ViolationObserved);
  BLACKBOX_CHECK(joiner.metrics().released_ordered_events == 4U);
  BLACKBOX_REQUIRE(joiner.firstViolation().has_value());
  BLACKBOX_CHECK(joiner.firstViolation()->session_id == session_one);
  BLACKBOX_CHECK(
      joiner.firstViolation()->checker_violation.session_lower_bound == 1U);
}

void testBootstrapApprovalMustMatchExactPrefix() {
  {
    TransactionJoiner joiner(config());
    joiner.acceptCdcTransaction(control(0U, RunFencePhase::RunStart));
    joiner.acceptCdcTransaction(control(1U, RunFencePhase::LoadFinished));
    auto wrong = emptyBootstrapProof(joiner);
    wrong.prefix.bootstrap_transactions = 1U;
    expectError(
        joiner, [&] { joiner.validateBootstrapState(wrong); },
        MonitorErrorCode::BootstrapStateMismatch);
    BLACKBOX_CHECK(joiner.metrics().checker.phase_anchor_transactions == 0U);
    BLACKBOX_CHECK(joiner.metrics().pending_cdc_slots == 1U);
  }
  {
    TransactionJoiner joiner(config());
    joiner.acceptCdcTransaction(control(0U, RunFencePhase::RunStart));
    joiner.acceptCdcTransaction(control(1U, RunFencePhase::LoadFinished));
    auto wrong = emptyBootstrapProof(joiner);
    wrong.canonical_state_digest = std::string(64U, '0');
    expectError(
        joiner, [&] { joiner.validateBootstrapState(wrong); },
        MonitorErrorCode::BootstrapStateMismatch);
    BLACKBOX_CHECK(joiner.metrics().checker.phase_anchor_transactions == 0U);
  }
}

void testDuplicateAndSessionInvariants() {
  const auto first = clientPayload(session_one, 1U, 1U, 1U);
  {
    TransactionJoiner joiner(config());
    joiner.acceptClientPayload(first);
    expectError(
        joiner, [&] { joiner.acceptClientPayload(first); },
        MonitorErrorCode::ClientDuplicateCorrelation);
  }
  {
    TransactionJoiner joiner(config());
    joiner.acceptClientPayload(first);
    expectError(
        joiner,
        [&] {
          joiner.acceptClientPayload(clientPayload(
              session_two, 1U, 1U, 2U, {}, std::nullopt, uuid(1U, '2')));
        },
        MonitorErrorCode::AttemptDuplicate);
  }
  {
    TransactionJoiner joiner(config());
    joiner.acceptClientPayload(first);
    expectError(
        joiner,
        [&] {
          joiner.acceptClientPayload(clientPayload(
              session_one, 1U, 2U, 2U, {}, uuid(1U, '1'), uuid(2U, '2')));
        },
        MonitorErrorCode::MultipleCommittedAttempts);
  }
  {
    TransactionJoiner joiner(config());
    joiner.acceptClientPayload(first);
    expectError(
        joiner,
        [&] {
          joiner.acceptClientPayload(clientPayload(
              session_two, 1U, 1U, 2U, {}, uuid(1U, '1'), uuid(2U, '2')));
        },
        MonitorErrorCode::LogicalTransactionDuplicate);
  }
  {
    TransactionJoiner joiner(config());
    joiner.acceptClientPayload(clientPayload(session_one, 2U, 2U, 1U));
    expectError(
        joiner,
        [&] {
          joiner.acceptClientPayload(clientPayload(session_one, 1U, 3U, 2U));
        },
        MonitorErrorCode::SessionSequenceNonIncreasing);
  }
  {
    TransactionJoiner joiner(config());
    joiner.acceptClientPayload(clientPayload(session_one, 1U, 1U, 1U));
    expectError(
        joiner,
        [&] {
          joiner.acceptClientPayload(clientPayload(session_one, 3U, 2U, 2U));
        },
        MonitorErrorCode::SessionAttemptProgressInvalid);
  }
  {
    TransactionJoiner joiner(config());
    joiner.acceptClientPayload(clientPayload(session_one, 1U, 3U, 1U));
    expectError(
        joiner,
        [&] {
          joiner.acceptClientPayload(clientPayload(session_one, 3U, 4U, 2U));
        },
        MonitorErrorCode::SessionAttemptProgressInvalid);
  }
  {
    TransactionJoiner joiner(config());
    joiner.acceptClientPayload(clientPayload(session_one, 1U, 2U, 1U));
    joiner.acceptClientPayload(clientPayload(session_one, 3U, 4U, 2U));
    BLACKBOX_CHECK(joiner.metrics().accepted_client_transactions == 2U);
  }
}

void testDuplicatesRemainRejectedAfterDrain() {
  const auto payload = clientPayload(session_one, 1U, 1U, 1U);
  {
    TransactionJoiner joiner(config());
    openWorkload(joiner);
    joiner.acceptClientPayload(payload);
    joiner.acceptCdcTransaction(workload(2U, session_one, 1U));
    BLACKBOX_CHECK(joiner.metrics().pending_client_records == 0U);
    expectError(
        joiner, [&] { joiner.acceptClientPayload(payload); },
        MonitorErrorCode::ClientDuplicateCorrelation);
  }
  {
    TransactionJoiner joiner(config());
    openWorkload(joiner);
    joiner.acceptClientPayload(payload);
    joiner.acceptCdcTransaction(workload(2U, session_one, 1U));
    expectError(
        joiner,
        [&] { joiner.acceptCdcTransaction(workload(3U, session_one, 1U)); },
        MonitorErrorCode::MarkerDuplicate);
  }
}

void testCdcAndClientEvidenceFailures() {
  {
    TransactionJoiner joiner(config());
    expectError(
        joiner,
        [&] {
          joiner.acceptCdcTransaction(control(1U, RunFencePhase::RunStart));
        },
        MonitorErrorCode::CdcOrderInvalid);
  }
  {
    TransactionJoiner joiner(config());
    openWorkload(joiner);
    joiner.acceptCdcTransaction(workload(2U, session_one, 1U));
    expectError(
        joiner,
        [&] { joiner.acceptCdcTransaction(workload(3U, session_one, 1U)); },
        MonitorErrorCode::MarkerDuplicate);
  }
  {
    TransactionJoiner joiner(config());
    openWorkload(joiner);
    expectError(
        joiner,
        [&] {
          joiner.acceptCdcTransaction(workload(2U, "ycsb/worker/unknown", 1U));
        },
        MonitorErrorCode::SessionUnknown);
  }
  {
    TransactionJoiner joiner(config());
    openWorkload(joiner);
    expectError(
        joiner,
        [&] {
          joiner.acceptCdcTransaction(
              workload(2U, session_one, 1U,
                       {presentWrite(2, "two"), presentWrite(1, "one")}));
        },
        MonitorErrorCode::CheckerInputRejected);
  }
  {
    TransactionJoiner joiner(config());
    expectError(
        joiner, [&] { joiner.acceptClientPayload("not-json"); },
        MonitorErrorCode::ClientRecordMalformed);
  }
}

void testBoundsAreCountedWithoutDroppingEvidence() {
  BLACKBOX_CHECK(TransactionJoinerLimits{}.max_pending_client_bytes ==
                 256U * 1024U * 1024U);
  BLACKBOX_CHECK(TransactionJoinerLimits{}.max_retained_identity_bytes ==
                 512U * 1024U * 1024U);
  auto invalid_limits = TransactionJoinerLimits{};
  invalid_limits.max_retained_identity_bytes = 0U;
  bool rejected_zero_identity_capacity = false;
  try {
    TransactionJoiner invalid(config(invalid_limits));
  } catch (const std::invalid_argument &) {
    rejected_zero_identity_capacity = true;
  }
  BLACKBOX_CHECK(rejected_zero_identity_capacity);

  TransactionJoiner baseline(config());
  baseline.acceptClientPayload(clientPayload(session_one, 1U, 1U, 1U));
  const auto exact_client_bytes = baseline.metrics().pending_client_bytes;
  const auto exact_identity_records =
      baseline.metrics().retained_identity_records;
  const auto exact_identity_bytes = baseline.metrics().retained_identity_bytes;
  BLACKBOX_CHECK(exact_client_bytes != 0U);

  TransactionJoiner one_read(config());
  one_read.acceptClientPayload(
      clientPayload(session_one, 1U, 1U, 1U, {absentRead(0U, 1)}));
  const auto one_read_bytes = one_read.metrics().pending_client_bytes;
  BLACKBOX_CHECK(one_read_bytes >= exact_client_bytes + sizeof(ClientRead) +
                                       key(1).size() + 128U);

  auto limits = TransactionJoinerLimits{};
  limits.max_pending_client_records = 1U;
  limits.max_pending_client_bytes = exact_client_bytes;
  TransactionJoiner exact(config(limits));
  exact.acceptClientPayload(clientPayload(session_one, 1U, 1U, 1U));
  BLACKBOX_CHECK(exact.metrics().pending_client_bytes == exact_client_bytes);
  expectError(
      exact,
      [&] {
        exact.acceptClientPayload(clientPayload(session_two, 1U, 1U, 2U));
      },
      MonitorErrorCode::JoinBufferLimitExceeded);
  BLACKBOX_CHECK(exact.metrics().pending_client_records == 1U);

  limits = TransactionJoinerLimits{};
  limits.max_pending_client_bytes = exact_client_bytes - 1U;
  TransactionJoiner too_small(config(limits));
  expectError(
      too_small,
      [&] {
        too_small.acceptClientPayload(clientPayload(session_one, 1U, 1U, 1U));
      },
      MonitorErrorCode::JoinBufferLimitExceeded);

  limits = TransactionJoinerLimits{};
  limits.max_retained_identity_records = exact_identity_records;
  limits.max_retained_identity_bytes = exact_identity_bytes;
  TransactionJoiner exact_identities(config(limits));
  exact_identities.acceptClientPayload(clientPayload(session_one, 1U, 1U, 1U));
  BLACKBOX_CHECK(exact_identities.metrics().retained_identity_bytes ==
                 exact_identity_bytes);

  limits.max_retained_identity_records = exact_identity_records - 1U;
  TransactionJoiner too_few_identities(config(limits));
  expectError(
      too_few_identities,
      [&] {
        too_few_identities.acceptClientPayload(
            clientPayload(session_one, 1U, 1U, 1U));
      },
      MonitorErrorCode::ResourceExhausted);

  limits = TransactionJoinerLimits{};
  limits.max_retained_identity_bytes = exact_identity_bytes - 1U;
  TransactionJoiner too_few_identity_bytes(config(limits));
  expectError(
      too_few_identity_bytes,
      [&] {
        too_few_identity_bytes.acceptClientPayload(
            clientPayload(session_one, 1U, 1U, 1U));
      },
      MonitorErrorCode::ResourceExhausted);

  limits = TransactionJoinerLimits{};
  limits.max_pending_cdc_slots = 1U;
  TransactionJoiner cdc_limited(config(limits));
  openWorkload(cdc_limited);
  cdc_limited.acceptCdcTransaction(workload(2U, session_one, 1U));
  expectError(
      cdc_limited,
      [&] { cdc_limited.acceptCdcTransaction(workload(3U, session_two, 1U)); },
      MonitorErrorCode::JoinBufferLimitExceeded);
  BLACKBOX_CHECK(cdc_limited.metrics().pending_cdc_slots == 1U);

  TransactionJoiner cdc_baseline(config());
  openWorkload(cdc_baseline);
  cdc_baseline.acceptCdcTransaction(workload(2U, session_one, 1U));
  const auto exact_cdc_bytes =
      cdc_baseline.metrics().pending_cdc_high_water_bytes;

  TransactionJoiner cdc_with_write(config());
  openWorkload(cdc_with_write);
  const auto written_value = value("one");
  cdc_with_write.acceptCdcTransaction(
      workload(2U, session_one, 1U, {presentWrite(1, "one")}));
  BLACKBOX_CHECK(cdc_with_write.metrics().pending_cdc_bytes >=
                 cdc_baseline.metrics().pending_cdc_bytes +
                     sizeof(NormalizedWrite) + key(1).size() +
                     written_value.size() + 192U);

  TransactionJoiner cdc_with_large_write(config());
  openWorkload(cdc_with_large_write);
  constexpr std::size_t large_value_size = 1024U * 1024U;
  cdc_with_large_write.acceptCdcTransaction(
      workload(2U, session_one, 1U,
               {NormalizedWrite{
                   .key = key(1),
                   .state = NormalizedValueState::Present,
                   .value = CanonicalBytes(large_value_size, std::byte{0})}}));
  BLACKBOX_CHECK(cdc_with_large_write.metrics().pending_cdc_bytes >=
                 cdc_baseline.metrics().pending_cdc_bytes + large_value_size +
                     4096U);
  limits = TransactionJoinerLimits{};
  limits.max_pending_cdc_bytes = exact_cdc_bytes;
  TransactionJoiner exact_cdc(config(limits));
  openWorkload(exact_cdc);
  exact_cdc.acceptCdcTransaction(workload(2U, session_one, 1U));
  BLACKBOX_CHECK(exact_cdc.metrics().pending_cdc_bytes <= exact_cdc_bytes);

  limits.max_pending_cdc_bytes = exact_cdc_bytes - 1U;
  TransactionJoiner too_few_cdc_bytes(config(limits));
  expectError(
      too_few_cdc_bytes,
      [&] {
        openWorkload(too_few_cdc_bytes);
        too_few_cdc_bytes.acceptCdcTransaction(workload(2U, session_one, 1U));
      },
      MonitorErrorCode::JoinBufferLimitExceeded);

  limits = TransactionJoinerLimits{};
  limits.max_events_per_transaction = 1U;
  TransactionJoiner event_limited(config(limits));
  openWorkload(event_limited);
  event_limited.acceptClientPayload(clientPayload(
      session_one, 1U, 1U, 1U, {absentRead(0U, 1), absentRead(1U, 2)}));
  expectError(
      event_limited,
      [&] {
        event_limited.acceptCdcTransaction(workload(2U, session_one, 1U));
      },
      MonitorErrorCode::ResourceExhausted);
}

void testCheckerLimitAndAnchorCollision() {
  {
    auto limits = TransactionJoinerLimits{};
    limits.max_bootstrap_state_bytes = 1U;
    TransactionJoiner joiner(config(limits));
    joiner.acceptCdcTransaction(control(0U, RunFencePhase::RunStart));
    expectError(
        joiner,
        [&] {
          joiner.acceptCdcTransaction(
              bootstrap(1U, {presentWrite(1, "loaded")}));
        },
        MonitorErrorCode::ResourceExhausted);
  }
  {
    auto limits = TransactionJoinerLimits{};
    limits.max_checker_state_bytes = 0U;
    TransactionJoiner joiner(config(limits));
    joiner.acceptCdcTransaction(control(0U, RunFencePhase::RunStart));
    expectError(
        joiner,
        [&] {
          joiner.acceptCdcTransaction(
              bootstrap(1U, {presentWrite(1, "loaded")}));
        },
        MonitorErrorCode::CheckerStateLimitExceeded);
  }
  {
    TransactionJoiner joiner(config());
    joiner.acceptClientPayload(
        clientPayload(session_one, 1U, 1U, 1U, {},
                      "626f6f74-7374-7270-0000-000000000001", std::nullopt));
    joiner.acceptCdcTransaction(control(0U, RunFencePhase::RunStart));
    expectError(
        joiner, [&] { joiner.acceptCdcTransaction(bootstrap(1U, {})); },
        MonitorErrorCode::InternalError);
    BLACKBOX_CHECK(joiner.metrics().checker.bootstrap_transactions == 0U);
  }
  {
    TransactionJoiner joiner(config());
    joiner.acceptCdcTransaction(control(0U, RunFencePhase::RunStart));
    joiner.acceptCdcTransaction(bootstrap(1U, {}));
    expectError(
        joiner,
        [&] {
          joiner.acceptClientPayload(
              clientPayload(session_one, 1U, 1U, 1U, {}, std::nullopt,
                            "626f6f74-6174-746d-0000-000000000001"));
        },
        MonitorErrorCode::InternalError);
  }
  {
    TransactionJoiner joiner(config());
    joiner.acceptClientPayload(
        clientPayload(session_one, 1U, 1U, 1U, {},
                      "70686173-6561-6e63-0000-000000000001", std::nullopt));
    joiner.acceptCdcTransaction(control(0U, RunFencePhase::RunStart));
    joiner.acceptCdcTransaction(control(1U, RunFencePhase::LoadFinished));
    const auto proof = emptyBootstrapProof(joiner);
    expectError(
        joiner, [&] { joiner.validateBootstrapState(proof); },
        MonitorErrorCode::InternalError);
    BLACKBOX_CHECK(joiner.metrics().checker.phase_anchor_transactions == 0U);
  }
}

void testIncrementalUnmatchedMetricsUnderLongPendingPrefix() {
  constexpr std::uint64_t transaction_count = 4096U;
  TransactionJoiner joiner(config());
  openWorkload(joiner);

  for (std::uint64_t sequence = 1U; sequence <= transaction_count; ++sequence) {
    joiner.acceptClientPayload(
        clientPayload(session_one, sequence, sequence, sequence));
    const auto metrics = joiner.metrics();
    BLACKBOX_CHECK(metrics.pending_client_records == sequence);
    BLACKBOX_CHECK(metrics.unmatched_client_records == sequence);
    BLACKBOX_CHECK(metrics.unmatched_workload_cdc_slots == 0U);
  }

  auto audit = joiner.auditReconciliationState();
  BLACKBOX_CHECK(audit.incremental_counters_match);
  BLACKBOX_CHECK(audit.unmatched_client_records == transaction_count);
  for (std::uint64_t sequence = 1U; sequence <= transaction_count; ++sequence) {
    joiner.acceptCdcTransaction(workload(sequence + 1U, session_one, sequence));
    const auto metrics = joiner.metrics();
    BLACKBOX_CHECK(metrics.unmatched_client_records ==
                   transaction_count - sequence);
    BLACKBOX_CHECK(metrics.unmatched_workload_cdc_slots == 0U);
  }
  audit = joiner.auditReconciliationState();
  BLACKBOX_CHECK(audit.incremental_counters_match);
  BLACKBOX_CHECK(audit.pending_client_records == 0U);
  BLACKBOX_CHECK(audit.pending_cdc_slots == 0U);
}

} // namespace

int runTransactionJoinerTests() {
  testStrictReadyPrefixAndEndDeferral();
  testHeadWaitClockAndCombinedJoinBufferPeak();
  testOrderedJoiningWithoutCheckerExecution();
  testEndFenceCannotPassMissingClient();
  testBootstrapGateAndAnchorLowerBound();
  testBootstrapApprovalMustMatchExactPrefix();
  testDuplicateAndSessionInvariants();
  testDuplicatesRemainRejectedAfterDrain();
  testCdcAndClientEvidenceFailures();
  testBoundsAreCountedWithoutDroppingEvidence();
  testCheckerLimitAndAnchorCollision();
  testIncrementalUnmatchedMetricsUnderLongPendingPrefix();
  return 0;
}
