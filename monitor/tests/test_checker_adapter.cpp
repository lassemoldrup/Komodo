#include "blackbox/checker_adapter.h"

#include "blackbox/canonical_ycsb_codec.h"
#include "test_support.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using blackbox::AttributedViolation;
using blackbox::CanonicalBytes;
using blackbox::CanonicalYcsbCodec;
using blackbox::CheckerAdapter;
using blackbox::CheckerAdapterConfig;
using blackbox::CheckerObservation;
using blackbox::ClientRead;
using blackbox::ClientReadSource;
using blackbox::ClientTxn;
using blackbox::ClientValueState;
using blackbox::MonitorError;
using blackbox::MonitorErrorCode;
using blackbox::NormalizedValueState;
using blackbox::NormalizedWrite;
using blackbox::RunScopedCorrelationId;

constexpr std::string_view run_id = "00112233-4455-6677-8899-aabbccddeeff";
constexpr std::string_view session_one = "ycsb/worker/1";
constexpr std::string_view session_two = "ycsb/worker/2";

[[nodiscard]] CheckerAdapter
adapter(const std::size_t state_limit = 512U * 1024U * 1024U) {
  return CheckerAdapter(CheckerAdapterConfig{
      .run_id = std::string(run_id),
      .expected_session_ids = {std::string(session_one),
                               std::string(session_two)},
      .max_retained_state_bytes = state_limit,
  });
}

[[nodiscard]] CanonicalBytes key(const std::int32_t value) {
  return CanonicalYcsbCodec::encodeKey("benchbase", "usertable", value);
}

[[nodiscard]] CanonicalBytes value(const std::string &field_one) {
  std::array<std::optional<std::string>, 10> fields{};
  fields[0] = field_one;
  return CanonicalYcsbCodec::encodeValue(fields);
}

[[nodiscard]] NormalizedWrite presentWrite(const std::int32_t key_value,
                                           const std::string &field_one) {
  return NormalizedWrite{.key = key(key_value),
                         .state = NormalizedValueState::Present,
                         .value = value(field_one)};
}

[[nodiscard]] NormalizedWrite absentWrite(const std::int32_t key_value) {
  return NormalizedWrite{.key = key(key_value),
                         .state = NormalizedValueState::Absent,
                         .value = std::nullopt};
}

[[nodiscard]] ClientRead
presentRead(const std::uint32_t sequence, const std::int32_t key_value,
            const std::string &field_one,
            const ClientReadSource source = ClientReadSource::Snapshot) {
  return ClientRead{.event_seq = sequence,
                    .key = key(key_value),
                    .state = ClientValueState::Present,
                    .value = value(field_one),
                    .source = source};
}

[[nodiscard]] ClientRead
absentRead(const std::uint32_t sequence, const std::int32_t key_value,
           const ClientReadSource source = ClientReadSource::Snapshot) {
  return ClientRead{.event_seq = sequence,
                    .key = key(key_value),
                    .state = ClientValueState::Absent,
                    .value = std::nullopt,
                    .source = source};
}

[[nodiscard]] ClientTxn client(
    const std::string_view session_id, const std::uint64_t session_seq,
    const std::uint64_t txn_id, std::vector<ClientRead> reads = {},
    const std::string_view logical_id = "11111111-1111-4111-8111-111111111111",
    const std::string_view attempt_id =
        "22222222-2222-4222-8222-222222222222") {
  return ClientTxn{
      .run_id = std::string(run_id),
      .session_id = std::string(session_id),
      .session_seq = session_seq,
      .logical_txn_id = std::string(logical_id),
      .attempt_id = std::string(attempt_id),
      .txn_type = "ycsb_update",
      .correlation_id =
          RunScopedCorrelationId{.run_id = std::string(run_id),
                                 .session_id = std::string(session_id),
                                 .txn_id = txn_id},
      .read_digest = {},
      .reads = std::move(reads),
  };
}

void expectError(CheckerAdapter &checker, const std::function<void()> &action,
                 const MonitorErrorCode expected) {
  try {
    action();
    BLACKBOX_CHECK(false);
  } catch (const MonitorError &error) {
    BLACKBOX_CHECK(error.code() == expected);
    BLACKBOX_CHECK(checker.failed());
    BLACKBOX_CHECK(checker.errorCode() == expected);
  }
}

void testIdentityMapping() {
  const auto bootstrap = blackbox::makeBootstrapCheckerIdentity(run_id, 7U);
  BLACKBOX_CHECK(bootstrap.transaction_id.words[0] == 0x626f6f7473747270ULL);
  BLACKBOX_CHECK(bootstrap.transaction_id.words[1] == 7U);
  BLACKBOX_CHECK(bootstrap.attempt_id.words[0] == 0x626f6f746174746dULL);
  BLACKBOX_CHECK(bootstrap.session_id.runtime_epoch.words[0] ==
                 0x0011223344556677ULL);
  BLACKBOX_CHECK(bootstrap.session_id.runtime_epoch.words[1] ==
                 0x8899aabbccddeeffULL);
  BLACKBOX_CHECK(bootstrap.session_id.producer_id == 0U);
  BLACKBOX_CHECK(bootstrap.session_id.local_session_counter == 8U);
  BLACKBOX_CHECK(bootstrap.session_seq == 1U);

  const auto anchor = blackbox::makePhaseAnchorCheckerIdentity(run_id, 2U);
  BLACKBOX_CHECK(anchor.transaction_id.words[0] == 0x7068617365616e63ULL);
  BLACKBOX_CHECK(anchor.transaction_id.words[1] == 2U);
  BLACKBOX_CHECK(anchor.attempt_id.words[0] == 0x7068617365617474ULL);
  BLACKBOX_CHECK(anchor.session_id.producer_id == 2U);
  BLACKBOX_CHECK(anchor.session_id.local_session_counter == 1U);
  BLACKBOX_CHECK(anchor.session_seq == 0U);
}

void testBootstrapVisibilityAndBorrowedLifetime() {
  auto checker = adapter();
  auto bootstrap_value = value("loaded");
  std::vector<NormalizedWrite> writes = {
      NormalizedWrite{.key = key(1),
                      .state = NormalizedValueState::Present,
                      .value = bootstrap_value}};
  checker.consumeBootstrap(0U, 1U, writes);
  writes[0].key.assign(1U, std::byte{0xff});
  writes[0].value->assign(1U, std::byte{0xff});
  checker.consumePhaseAnchor(1U, session_one);
  checker.consumeWorkload(
      2U, client(session_one, 1U, 1U, {presentRead(0U, 1, "loaded")}), {});
  BLACKBOX_CHECK(checker.observation() == CheckerObservation::PassSoFar);
  const auto metrics = checker.metrics();
  BLACKBOX_CHECK(metrics.next_commit_seq == 3U);
  BLACKBOX_CHECK(metrics.consumed_transactions == 3U);
  BLACKBOX_CHECK(metrics.bootstrap_transactions == 1U);
  BLACKBOX_CHECK(metrics.phase_anchor_transactions == 1U);
  BLACKBOX_CHECK(metrics.workload_transactions == 1U);
}

void testAnchorLowerBoundAndViolationAttribution() {
  auto checker = adapter();
  checker.consumeBootstrap(0U, 1U, {presentWrite(1, "loaded")});
  checker.consumePhaseAnchor(1U, session_one);
  checker.consumePhaseAnchor(2U, session_two);
  const auto violating = client(session_one, 1U, 1U, {absentRead(0U, 1)},
                                "33333333-3333-4333-8333-333333333333",
                                "44444444-4444-4444-8444-444444444444");
  checker.consumeWorkload(3U, violating, {});
  BLACKBOX_CHECK(checker.observation() ==
                 CheckerObservation::ViolationObserved);
  BLACKBOX_REQUIRE(checker.firstViolation().has_value());
  const AttributedViolation captured = *checker.firstViolation();
  BLACKBOX_CHECK(captured.run_id == run_id);
  BLACKBOX_CHECK(captured.session_id == session_one);
  BLACKBOX_CHECK(captured.logical_txn_id == violating.logical_txn_id);
  BLACKBOX_CHECK(captured.attempt_id == violating.attempt_id);
  BLACKBOX_CHECK(captured.correlation_id == violating.correlation_id);
  BLACKBOX_CHECK(captured.session_seq == 1U);
  BLACKBOX_CHECK(captured.checker_violation.commit_seq == 3U);
  BLACKBOX_CHECK(captured.checker_violation.session_lower_bound == 1U);
  BLACKBOX_CHECK(captured.checker_violation.kind ==
                 co_checker::online::ViolationKind::NoCompatibleSnapshot);

  checker.consumeWorkload(4U,
                          client(session_two, 1U, 1U,
                                 {presentRead(0U, 1, "loaded")},
                                 "55555555-5555-4555-8555-555555555555",
                                 "66666666-6666-4666-8666-666666666666"),
                          {});
  BLACKBOX_CHECK(checker.observation() ==
                 CheckerObservation::ViolationObserved);
  BLACKBOX_CHECK(checker.firstViolation() == captured);
  BLACKBOX_CHECK(checker.metrics().next_commit_seq == 5U);
}

void testMarkerOnlyLocalReadsAndDeletes() {
  auto checker = adapter();
  checker.consumeBootstrap(0U, 1U, {presentWrite(1, "loaded")});
  checker.consumePhaseAnchor(1U, session_one);
  checker.consumePhaseAnchor(2U, session_two);
  checker.consumeWorkload(3U, client(session_one, 1U, 1U), {});
  checker.consumeWorkload(4U,
                          client(session_two, 1U, 1U,
                                 {absentRead(0U, 1, ClientReadSource::Local)},
                                 "33333333-3333-4333-8333-333333333333",
                                 "44444444-4444-4444-8444-444444444444"),
                          {absentWrite(1)});
  checker.consumeWorkload(5U,
                          client(session_one, 2U, 2U, {absentRead(0U, 1)},
                                 "55555555-5555-4555-8555-555555555555",
                                 "66666666-6666-4666-8666-666666666666"),
                          {});
  BLACKBOX_CHECK(checker.observation() == CheckerObservation::PassSoFar);
  BLACKBOX_CHECK(checker.metrics().workload_transactions == 3U);
}

void testConflictingRepeatedReads() {
  auto checker = adapter();
  checker.consumePhaseAnchor(0U, session_one);
  checker.consumeWorkload(
      1U,
      client(session_one, 1U, 1U,
             {presentRead(0U, 1, "one"), presentRead(1U, 1, "two")}),
      {});
  BLACKBOX_REQUIRE(checker.firstViolation().has_value());
  const auto &violation = checker.firstViolation()->checker_violation;
  BLACKBOX_CHECK(violation.kind ==
                 co_checker::online::ViolationKind::InconsistentRepeatedRead);
  BLACKBOX_CHECK(violation.first_event_sequence == 0U);
  BLACKBOX_CHECK(violation.conflicting_event_sequence == 1U);
}

void testInputAndSequenceErrorsAreSticky() {
  {
    auto checker = adapter();
    expectError(
        checker, [&] { checker.consumePhaseAnchor(1U, session_one); },
        MonitorErrorCode::CommitSequenceInvalid);
    expectError(
        checker, [&] { checker.consumePhaseAnchor(0U, session_one); },
        MonitorErrorCode::CommitSequenceInvalid);
  }
  {
    auto checker = adapter();
    checker.consumePhaseAnchor(0U, session_one);
    expectError(
        checker, [&] { checker.consumePhaseAnchor(1U, session_one); },
        MonitorErrorCode::PhaseAnchorFailed);
  }
  {
    auto checker = adapter();
    expectError(
        checker, [&] { checker.consumePhaseAnchor(0U, "ycsb/worker/unknown"); },
        MonitorErrorCode::PhaseAnchorFailed);
  }
  {
    auto checker = adapter();
    std::vector<NormalizedWrite> writes = {presentWrite(2, "two"),
                                           presentWrite(1, "one")};
    expectError(
        checker, [&] { checker.consumeBootstrap(0U, 1U, writes); },
        MonitorErrorCode::CheckerInputRejected);
  }
  {
    auto checker = adapter();
    auto malformed = presentWrite(1, "one");
    malformed.value.reset();
    expectError(
        checker, [&] { checker.consumeBootstrap(0U, 1U, {malformed}); },
        MonitorErrorCode::CheckerInputRejected);
  }
  {
    auto checker = adapter();
    checker.consumePhaseAnchor(0U, session_one);
    checker.consumeWorkload(1U, client(session_one, 1U, 1U), {});
    expectError(
        checker,
        [&] {
          checker.consumeWorkload(
              2U,
              client(session_one, 1U, 2U, {},
                     "33333333-3333-4333-8333-333333333333",
                     "44444444-4444-4444-8444-444444444444"),
              {});
        },
        MonitorErrorCode::SessionSequenceNonIncreasing);
  }
}

void testStateLimitAndIdentityCollision() {
  {
    auto checker = adapter(0U);
    expectError(
        checker,
        [&] { checker.consumeBootstrap(0U, 1U, {presentWrite(1, "one")}); },
        MonitorErrorCode::CheckerStateLimitExceeded);
  }
  {
    auto checker = adapter();
    checker.consumeBootstrap(0U, 5U, {});
    checker.consumePhaseAnchor(1U, session_one);
    const auto collision =
        client(session_one, 1U, 1U, {}, "626f6f74-7374-7270-0000-000000000005",
               "33333333-3333-4333-8333-333333333333");
    expectError(
        checker, [&] { checker.consumeWorkload(2U, collision, {}); },
        MonitorErrorCode::InternalError);
  }
}

} // namespace

int runCheckerAdapterTests() {
  testIdentityMapping();
  testBootstrapVisibilityAndBorrowedLifetime();
  testAnchorLowerBoundAndViolationAttribution();
  testMarkerOnlyLocalReadsAndDeletes();
  testConflictingRepeatedReads();
  testInputAndSequenceErrorsAreSticky();
  testStateLimitAndIdentityCollision();
  return 0;
}
