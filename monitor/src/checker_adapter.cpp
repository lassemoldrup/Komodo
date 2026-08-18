#include "blackbox/checker_adapter.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace blackbox {
namespace {

using co_checker::online::ByteView;
using co_checker::online::CommitOrderToken;
using co_checker::online::EngineErrorCode;
using co_checker::online::EntityUid;
using co_checker::online::EventOp;
using co_checker::online::EventView;
using co_checker::online::OrderedTxnMeta;
using co_checker::online::OrderTokenKind;
using co_checker::online::ReadSource;
using co_checker::online::RuntimeEpoch;
using co_checker::online::SessionUid;
using co_checker::online::ValueState;

constexpr std::uint64_t bootstrap_transaction_tag = 0x626f6f7473747270ULL;
constexpr std::uint64_t bootstrap_attempt_tag = 0x626f6f746174746dULL;
constexpr std::uint64_t phase_anchor_transaction_tag = 0x7068617365616e63ULL;
constexpr std::uint64_t phase_anchor_attempt_tag = 0x7068617365617474ULL;

[[nodiscard]] std::uint64_t steadyClockNanoseconds() noexcept {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

[[nodiscard]] bool isLowerUuid(const std::string_view value) {
  if (value.size() != 36U) {
    return false;
  }
  for (std::size_t index = 0; index < value.size(); ++index) {
    if (index == 8U || index == 13U || index == 18U || index == 23U) {
      if (value[index] != '-') {
        return false;
      }
      continue;
    }
    if (!(value[index] >= '0' && value[index] <= '9') &&
        !(value[index] >= 'a' && value[index] <= 'f')) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] unsigned int hexValue(const char character) {
  if (character >= '0' && character <= '9') {
    return static_cast<unsigned int>(character - '0');
  }
  return static_cast<unsigned int>(character - 'a' + 10);
}

[[nodiscard]] std::array<std::uint64_t, 2>
uuidWords(const std::string_view uuid) {
  if (!isLowerUuid(uuid)) {
    throw std::invalid_argument("checker identity is not a lowercase UUID");
  }
  std::array<std::uint64_t, 2> words{};
  std::size_t byte_index = 0;
  for (std::size_t index = 0; index < uuid.size();) {
    if (uuid[index] == '-') {
      ++index;
      continue;
    }
    const auto byte =
        (hexValue(uuid[index]) << 4U) | hexValue(uuid[index + 1U]);
    const auto word_index = byte_index / 8U;
    words[word_index] = (words[word_index] << 8U) | byte;
    ++byte_index;
    index += 2U;
  }
  return words;
}

[[nodiscard]] RuntimeEpoch runtimeEpoch(const std::string_view run_id) {
  return RuntimeEpoch{.words = uuidWords(run_id)};
}

[[nodiscard]] EntityUid entity(const std::string_view uuid) {
  return EntityUid{.words = uuidWords(uuid)};
}

[[nodiscard]] EntityUid taggedEntity(const std::uint64_t tag,
                                     const std::uint64_t sequence) {
  return EntityUid{.words = {tag, sequence}};
}

struct EntityHash {
  [[nodiscard]] std::size_t
  operator()(const EntityUid &identity) const noexcept {
    const auto first = std::hash<std::uint64_t>{}(identity.words[0]);
    const auto second = std::hash<std::uint64_t>{}(identity.words[1]);
    return first ^
           (second + 0x9e3779b97f4a7c15ULL + (first << 6U) + (first >> 2U));
  }
};

[[nodiscard]] bool bytesLess(const CanonicalBytes &left,
                             const CanonicalBytes &right) {
  return std::lexicographical_compare(
      left.begin(), left.end(), right.begin(), right.end(),
      [](const std::byte lhs, const std::byte rhs) {
        return std::to_integer<unsigned int>(lhs) <
               std::to_integer<unsigned int>(rhs);
      });
}

[[nodiscard]] MonitorErrorCode checkerErrorCode(const EngineErrorCode code,
                                                const bool workload) {
  switch (code) {
  case EngineErrorCode::UnexpectedCommitSequence:
    return MonitorErrorCode::CommitSequenceInvalid;
  case EngineErrorCode::RetainedStateLimitExceeded:
    return MonitorErrorCode::CheckerStateLimitExceeded;
  case EngineErrorCode::ResourceExhausted:
    return MonitorErrorCode::ResourceExhausted;
  case EngineErrorCode::NonIncreasingSessionSequence:
    return workload ? MonitorErrorCode::SessionSequenceNonIncreasing
                    : MonitorErrorCode::CheckerInputRejected;
  case EngineErrorCode::NonIncreasingEventSequence:
  case EngineErrorCode::InvalidEventOperation:
  case EngineErrorCode::InvalidValueState:
  case EngineErrorCode::InvalidReadSource:
  case EngineErrorCode::AbsentValueHasPayload:
    return MonitorErrorCode::CheckerInputRejected;
  }
  return MonitorErrorCode::InternalError;
}

} // namespace

CheckerTransactionIdentity
makeBootstrapCheckerIdentity(const std::string_view run_id,
                             const std::uint64_t cdc_seq) {
  if (cdc_seq == std::numeric_limits<std::uint64_t>::max()) {
    throw std::invalid_argument("bootstrap cdc_seq cannot be incremented");
  }
  return CheckerTransactionIdentity{
      .transaction_id = taggedEntity(bootstrap_transaction_tag, cdc_seq),
      .attempt_id = taggedEntity(bootstrap_attempt_tag, cdc_seq),
      .session_id = SessionUid{.runtime_epoch = runtimeEpoch(run_id),
                               .producer_id = 0,
                               .local_session_counter = cdc_seq + 1U},
      .session_seq = 1,
  };
}

CheckerTransactionIdentity
makePhaseAnchorCheckerIdentity(const std::string_view run_id,
                               const std::uint32_t session_ordinal) {
  if (session_ordinal == 0U) {
    throw std::invalid_argument("phase-anchor session ordinal must be nonzero");
  }
  return CheckerTransactionIdentity{
      .transaction_id =
          taggedEntity(phase_anchor_transaction_tag, session_ordinal),
      .attempt_id = taggedEntity(phase_anchor_attempt_tag, session_ordinal),
      .session_id = SessionUid{.runtime_epoch = runtimeEpoch(run_id),
                               .producer_id = session_ordinal,
                               .local_session_counter = 1},
      .session_seq = 0,
  };
}

class CheckerAdapter::Impl final {
public:
  explicit Impl(CheckerAdapterConfig config)
      : config_(std::move(config)), run_epoch_(runtimeEpoch(config_.run_id)),
        engine_(co_checker::online::SnapshotSOWWOptions{
            .retained_state_capacity_bytes =
                config_.max_retained_state_bytes}) {
    monotonic_clock_nanoseconds_ = config_.monotonic_clock_nanoseconds;
    if (!monotonic_clock_nanoseconds_) {
      monotonic_clock_nanoseconds_ = steadyClockNanoseconds;
    }
    if (config_.expected_session_ids.empty() ||
        config_.expected_session_ids.size() >
            std::numeric_limits<std::uint32_t>::max() ||
        config_.max_events_per_transaction == 0U ||
        config_.max_mapped_identity_records == 0U ||
        config_.max_mapped_identity_bytes == 0U ||
        config_.max_retained_state_bytes ==
            std::numeric_limits<std::size_t>::max()) {
      throw std::invalid_argument("checker expected-session roster is invalid");
    }
    for (std::size_t index = 0; index < config_.expected_session_ids.size();
         ++index) {
      const auto ordinal = static_cast<std::uint32_t>(index + 1U);
      if (config_.expected_session_ids[index].empty() ||
          !session_ordinals_
               .emplace(config_.expected_session_ids[index], ordinal)
               .second) {
        throw std::invalid_argument(
            "checker expected-session roster is empty or duplicated");
      }
    }
  }

  void consumeBootstrap(const std::uint64_t commit_seq,
                        const std::uint64_t cdc_seq,
                        const std::vector<NormalizedWrite> &writes) {
    guarded([&] {
      const auto identity =
          makeBootstrapCheckerIdentity(config_.run_id, cdc_seq);
      reserveIdentity(transaction_identities_, identity.transaction_id, true,
                      MonitorErrorCode::LogicalTransactionDuplicate);
      reserveIdentity(attempt_identities_, identity.attempt_id, true,
                      MonitorErrorCode::AttemptDuplicate);
      consume(commit_seq, identity, {}, writes, nullptr);
      ++bootstrap_transactions_;
    });
  }

  void consumePhaseAnchor(const std::uint64_t commit_seq,
                          const std::string_view session_id) {
    guarded([&] {
      try {
        const auto found = session_ordinals_.find(std::string(session_id));
        if (found == session_ordinals_.end()) {
          fail(MonitorErrorCode::SessionUnknown,
               "phase anchor names a session absent from the roster");
        }
        const auto identity =
            makePhaseAnchorCheckerIdentity(config_.run_id, found->second);
        reserveIdentity(transaction_identities_, identity.transaction_id, true,
                        MonitorErrorCode::LogicalTransactionDuplicate);
        reserveIdentity(attempt_identities_, identity.attempt_id, true,
                        MonitorErrorCode::AttemptDuplicate);
        consume(commit_seq, identity, {}, {}, nullptr);
      } catch (const MonitorError &error) {
        if (error.code() == MonitorErrorCode::ResourceExhausted ||
            error.code() == MonitorErrorCode::CheckerStateLimitExceeded ||
            error.code() == MonitorErrorCode::CommitSequenceInvalid) {
          throw;
        }
        throw MonitorError(MonitorErrorCode::PhaseAnchorFailed,
                           std::string("checker rejected phase anchor: ") +
                               error.what());
      }
      ++phase_anchor_transactions_;
    });
  }

  void consumeWorkload(const std::uint64_t commit_seq, const ClientTxn &client,
                       const std::vector<NormalizedWrite> &writes) {
    guarded([&] {
      if (client.run_id != config_.run_id ||
          client.correlation_id.run_id != config_.run_id) {
        fail(MonitorErrorCode::RunIdMismatch,
             "checker workload belongs to another run");
      }
      if (client.session_id != client.correlation_id.session_id) {
        fail(MonitorErrorCode::CorrelationMismatch,
             "checker workload correlation disagrees with its session");
      }
      const auto found = session_ordinals_.find(client.session_id);
      if (found == session_ordinals_.end()) {
        fail(MonitorErrorCode::SessionUnknown,
             "checker workload names a session absent from the roster");
      }
      if (client.session_seq == 0U) {
        fail(MonitorErrorCode::CheckerInputRejected,
             "real workload session_seq must be positive");
      }
      CheckerTransactionIdentity identity{
          .transaction_id = entity(client.logical_txn_id),
          .attempt_id = entity(client.attempt_id),
          .session_id = SessionUid{.runtime_epoch = run_epoch_,
                                   .producer_id = found->second,
                                   .local_session_counter = 1},
          .session_seq = client.session_seq,
      };
      reserveIdentity(transaction_identities_, identity.transaction_id, false,
                      MonitorErrorCode::LogicalTransactionDuplicate);
      reserveIdentity(attempt_identities_, identity.attempt_id, false,
                      MonitorErrorCode::AttemptDuplicate);
      consume(commit_seq, identity, client.reads, writes, &client);
      ++workload_transactions_;
    });
  }

  [[nodiscard]] CheckerObservation observation() const noexcept {
    return observation_;
  }

  [[nodiscard]] const std::optional<AttributedViolation> &
  firstViolation() const noexcept {
    return first_violation_;
  }

  [[nodiscard]] CheckerAdapterMetrics metrics() const {
    return CheckerAdapterMetrics{
        .next_commit_seq = next_commit_seq_,
        .consumed_transactions = consumed_transactions_,
        .bootstrap_transactions = bootstrap_transactions_,
        .phase_anchor_transactions = phase_anchor_transactions_,
        .workload_transactions = workload_transactions_,
        .read_events = read_events_,
        .write_events = write_events_,
        .consume_total_nanoseconds = consume_total_nanoseconds_,
        .consume_max_nanoseconds = consume_max_nanoseconds_,
        .mapped_identity_records = mapped_identity_records_,
        .mapped_identity_bytes = mapped_identity_bytes_,
        .engine = engine_.metrics(),
    };
  }

  [[nodiscard]] bool failed() const noexcept { return failure_.has_value(); }

  [[nodiscard]] std::optional<MonitorErrorCode> errorCode() const noexcept {
    if (!failure_.has_value()) {
      return std::nullopt;
    }
    return failure_->code;
  }

private:
  struct Failure {
    MonitorErrorCode code;
    std::string message;
  };

  struct IdentityOrigin {
    bool synthetic = false;
  };

  using IdentityMap = std::unordered_map<EntityUid, IdentityOrigin, EntityHash>;

  [[noreturn]] void fail(const MonitorErrorCode code,
                         const std::string &message) const {
    throw MonitorError(code, message);
  }

  void rememberFailure(const MonitorError &error) {
    if (!failure_.has_value()) {
      failure_ = Failure{.code = error.code(), .message = error.what()};
    }
  }

  template <typename Action> void guarded(Action &&action) {
    if (failure_.has_value()) {
      throw MonitorError(failure_->code, failure_->message);
    }
    try {
      std::forward<Action>(action)();
    } catch (const MonitorError &error) {
      rememberFailure(error);
      throw;
    } catch (const std::bad_alloc &) {
      const MonitorError error(MonitorErrorCode::ResourceExhausted,
                               "allocation failed at the checker boundary");
      rememberFailure(error);
      throw error;
    } catch (const std::length_error &) {
      const MonitorError error(
          MonitorErrorCode::ResourceExhausted,
          "container limit failed at the checker boundary");
      rememberFailure(error);
      throw error;
    } catch (const std::exception &error) {
      const MonitorError wrapped(
          MonitorErrorCode::InternalError,
          std::string("unexpected checker adapter failure: ") + error.what());
      rememberFailure(wrapped);
      throw wrapped;
    }
  }

  void reserveIdentity(IdentityMap &identities, const EntityUid &identity,
                       const bool synthetic,
                       const MonitorErrorCode duplicate_code) {
    const auto existing = identities.find(identity);
    if (existing != identities.end()) {
      if (existing->second.synthetic != synthetic || synthetic) {
        fail(MonitorErrorCode::InternalError,
             "external and reserved checker identities collide");
      }
      fail(duplicate_code, "external checker identity is duplicated");
    }
    constexpr std::size_t charge = sizeof(IdentityMap::value_type) + 64U;
    if (mapped_identity_records_ >= config_.max_mapped_identity_records ||
        charge > config_.max_mapped_identity_bytes ||
        mapped_identity_bytes_ > config_.max_mapped_identity_bytes - charge) {
      fail(MonitorErrorCode::ResourceExhausted,
           "mapped checker identities exceed their configured limit");
    }
    const auto [unused, inserted] =
        identities.emplace(identity, IdentityOrigin{.synthetic = synthetic});
    (void)unused;
    if (!inserted) {
      fail(MonitorErrorCode::InternalError,
           "checker identity insertion contradicted its preflight");
    }
    ++mapped_identity_records_;
    mapped_identity_bytes_ += charge;
  }

  void validateWrites(const std::span<const NormalizedWrite> writes) const {
    for (std::size_t index = 0; index < writes.size(); ++index) {
      const auto &write = writes[index];
      if (write.key.empty()) {
        fail(MonitorErrorCode::CheckerInputRejected,
             "normalized write has an empty key");
      }
      switch (write.state) {
      case NormalizedValueState::Present:
        if (!write.value.has_value()) {
          fail(MonitorErrorCode::CheckerInputRejected,
               "present normalized write lacks a value");
        }
        break;
      case NormalizedValueState::Absent:
        if (write.value.has_value()) {
          fail(MonitorErrorCode::CheckerInputRejected,
               "absent normalized write carries a value");
        }
        break;
      default:
        fail(MonitorErrorCode::CheckerInputRejected,
             "normalized write has an invalid value state");
      }
      if (index != 0U && !bytesLess(writes[index - 1U].key, write.key)) {
        fail(MonitorErrorCode::CheckerInputRejected,
             "normalized writes are duplicated or not canonically ordered");
      }
    }
  }

  [[nodiscard]] std::vector<EventView>
  makeEvents(const std::span<const ClientRead> reads,
             const std::span<const NormalizedWrite> writes) const {
    validateWrites(writes);
    const auto max_events =
        static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max()) +
        1U;
    if (reads.size() > config_.max_events_per_transaction ||
        writes.size() > config_.max_events_per_transaction - reads.size() ||
        reads.size() > std::numeric_limits<std::uint32_t>::max() ||
        writes.size() > std::numeric_limits<std::uint32_t>::max() ||
        reads.size() > max_events ||
        writes.size() > max_events - reads.size()) {
      fail(MonitorErrorCode::ResourceExhausted,
           "checker event count exceeds EventSeq range");
    }

    std::vector<EventView> events;
    events.reserve(reads.size() + writes.size());
    for (std::size_t index = 0; index < reads.size(); ++index) {
      const auto &read = reads[index];
      if (read.event_seq != index || read.key.empty()) {
        fail(MonitorErrorCode::CheckerInputRejected,
             "client reads lack dense order or a nonempty key");
      }
      ValueState state;
      switch (read.state) {
      case ClientValueState::Present:
        if (!read.value.has_value()) {
          fail(MonitorErrorCode::CheckerInputRejected,
               "present client read lacks a value");
        }
        state = ValueState::Present;
        break;
      case ClientValueState::Absent:
        if (read.value.has_value()) {
          fail(MonitorErrorCode::CheckerInputRejected,
               "absent client read carries a value");
        }
        state = ValueState::Absent;
        break;
      default:
        fail(MonitorErrorCode::CheckerInputRejected,
             "client read has an invalid value state");
      }
      ReadSource source;
      switch (read.source) {
      case ClientReadSource::Snapshot:
        source = ReadSource::Snapshot;
        break;
      case ClientReadSource::Local:
        source = ReadSource::Local;
        break;
      default:
        fail(MonitorErrorCode::CheckerInputRejected,
             "client read has an invalid source");
      }
      events.push_back(EventView{
          .sequence = static_cast<std::uint32_t>(index),
          .op = EventOp::Read,
          .value_state = state,
          .read_source = source,
          .key = ByteView(read.key.data(), read.key.size()),
          .value = read.value.has_value()
                       ? ByteView(read.value->data(), read.value->size())
                       : ByteView{},
      });
    }
    for (std::size_t index = 0; index < writes.size(); ++index) {
      const auto &write = writes[index];
      const auto sequence = reads.size() + index;
      events.push_back(EventView{
          .sequence = static_cast<std::uint32_t>(sequence),
          .op = EventOp::Write,
          .value_state = write.state == NormalizedValueState::Present
                             ? ValueState::Present
                             : ValueState::Absent,
          .read_source = ReadSource::Snapshot,
          .key = ByteView(write.key.data(), write.key.size()),
          .value = write.value.has_value()
                       ? ByteView(write.value->data(), write.value->size())
                       : ByteView{},
      });
    }
    return events;
  }

  void recordConsumeMetrics(const std::size_t read_count,
                            const std::size_t write_count,
                            const std::uint64_t start_nanoseconds,
                            const std::uint64_t end_nanoseconds) noexcept {
    const auto duration = end_nanoseconds >= start_nanoseconds
                              ? end_nanoseconds - start_nanoseconds
                              : 0U;
    const auto saturating_add = [](const std::uint64_t current,
                                   const std::uint64_t additional) {
      return additional > std::numeric_limits<std::uint64_t>::max() - current
                 ? std::numeric_limits<std::uint64_t>::max()
                 : current + additional;
    };
    read_events_ =
        saturating_add(read_events_, static_cast<std::uint64_t>(read_count));
    write_events_ =
        saturating_add(write_events_, static_cast<std::uint64_t>(write_count));
    consume_total_nanoseconds_ =
        saturating_add(consume_total_nanoseconds_, duration);
    consume_max_nanoseconds_ = std::max(consume_max_nanoseconds_, duration);
  }

  void consume(const std::uint64_t commit_seq,
               const CheckerTransactionIdentity &identity,
               const std::span<const ClientRead> reads,
               const std::span<const NormalizedWrite> writes,
               const ClientTxn *external) {
    if (commit_seq != next_commit_seq_ ||
        commit_seq != engine_.next_expected_commit_seq()) {
      fail(MonitorErrorCode::CommitSequenceInvalid,
           "checker commit_seq is not zero-based and dense");
    }
    if (commit_seq == std::numeric_limits<std::uint64_t>::max()) {
      fail(MonitorErrorCode::ResourceExhausted,
           "checker commit sequence space is exhausted");
    }
    const auto events = makeEvents(reads, writes);
    const OrderedTxnMeta metadata{
        .commit_seq = commit_seq,
        .transaction_id = identity.transaction_id,
        .attempt_id = identity.attempt_id,
        .session_id = identity.session_id,
        .session_seq = identity.session_seq,
        .commit_token =
            CommitOrderToken{
                .cluster_epoch = 0,
                .timestamp = commit_seq,
                .authoritative_tie_breaker = 0,
            },
        .order_token_kind = OrderTokenKind::WriteCommit,
        .publication_seq = co_checker::online::kUnassignedPublicationSeq,
        .producer_id = identity.session_id.producer_id,
        .producer_seq = identity.session_seq,
    };
    const auto consume_start = monotonic_clock_nanoseconds_();
    const auto result = engine_.consume(metadata, events);
    const auto consume_end = monotonic_clock_nanoseconds_();
    recordConsumeMetrics(reads.size(), writes.size(), consume_start,
                         consume_end);
    if (result.error.has_value()) {
      fail(checkerErrorCode(result.error->code, external != nullptr),
           std::string("SnapshotSOWWEngine rejected transaction: ") +
               result.error->message);
    }
    if (!result.verdict.has_value()) {
      fail(MonitorErrorCode::InternalError,
           "SnapshotSOWWEngine returned neither verdict nor error");
    }
    ++next_commit_seq_;
    ++consumed_transactions_;
    observation_ = CheckerObservation::PassSoFar;
    if (result.transaction_violation.has_value()) {
      if (external == nullptr) {
        fail(MonitorErrorCode::InternalError,
             "synthetic checker transaction produced a violation");
      }
      if (result.transaction_violation->commit_seq != commit_seq ||
          result.transaction_violation->transaction_id !=
              identity.transaction_id ||
          result.transaction_violation->session_id != identity.session_id ||
          result.transaction_violation->session_seq != identity.session_seq) {
        fail(MonitorErrorCode::InternalError,
             "checker violation identity disagrees with consumed workload");
      }
      observation_ = CheckerObservation::ViolationObserved;
      if (!first_violation_.has_value()) {
        first_violation_ = AttributedViolation{
            .run_id = external->run_id,
            .session_id = external->session_id,
            .logical_txn_id = external->logical_txn_id,
            .attempt_id = external->attempt_id,
            .correlation_id = external->correlation_id,
            .session_seq = external->session_seq,
            .checker_violation = *result.transaction_violation,
        };
      }
    } else if (first_violation_.has_value()) {
      observation_ = CheckerObservation::ViolationObserved;
    }
  }

  CheckerAdapterConfig config_;
  RuntimeEpoch run_epoch_;
  std::unordered_map<std::string, std::uint32_t> session_ordinals_;
  co_checker::online::SnapshotSOWWEngine engine_;
  std::function<std::uint64_t()> monotonic_clock_nanoseconds_;
  IdentityMap transaction_identities_;
  IdentityMap attempt_identities_;
  CheckerObservation observation_ = CheckerObservation::NotStarted;
  std::optional<AttributedViolation> first_violation_;
  std::optional<Failure> failure_;
  std::uint64_t next_commit_seq_ = 0;
  std::uint64_t consumed_transactions_ = 0;
  std::uint64_t bootstrap_transactions_ = 0;
  std::uint64_t phase_anchor_transactions_ = 0;
  std::uint64_t workload_transactions_ = 0;
  std::uint64_t read_events_ = 0;
  std::uint64_t write_events_ = 0;
  std::uint64_t consume_total_nanoseconds_ = 0;
  std::uint64_t consume_max_nanoseconds_ = 0;
  std::size_t mapped_identity_records_ = 0;
  std::size_t mapped_identity_bytes_ = 0;
};

CheckerAdapter::CheckerAdapter(CheckerAdapterConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

CheckerAdapter::~CheckerAdapter() = default;
CheckerAdapter::CheckerAdapter(CheckerAdapter &&) noexcept = default;
CheckerAdapter &CheckerAdapter::operator=(CheckerAdapter &&) noexcept = default;

void CheckerAdapter::consumeBootstrap(
    const std::uint64_t commit_seq, const std::uint64_t cdc_seq,
    const std::vector<NormalizedWrite> &writes) {
  impl_->consumeBootstrap(commit_seq, cdc_seq, writes);
}

void CheckerAdapter::consumePhaseAnchor(const std::uint64_t commit_seq,
                                        const std::string_view session_id) {
  impl_->consumePhaseAnchor(commit_seq, session_id);
}

void CheckerAdapter::consumeWorkload(
    const std::uint64_t commit_seq, const ClientTxn &client,
    const std::vector<NormalizedWrite> &writes) {
  impl_->consumeWorkload(commit_seq, client, writes);
}

CheckerObservation CheckerAdapter::observation() const noexcept {
  return impl_->observation();
}

const std::optional<AttributedViolation> &
CheckerAdapter::firstViolation() const noexcept {
  return impl_->firstViolation();
}

CheckerAdapterMetrics CheckerAdapter::metrics() const {
  return impl_->metrics();
}

bool CheckerAdapter::failed() const noexcept { return impl_->failed(); }

std::optional<MonitorErrorCode> CheckerAdapter::errorCode() const noexcept {
  return impl_->errorCode();
}

std::string_view toString(const CheckerObservation observation) noexcept {
  switch (observation) {
  case CheckerObservation::NotStarted:
    return "NOT_STARTED";
  case CheckerObservation::PassSoFar:
    return "PASS_SO_FAR";
  case CheckerObservation::ViolationObserved:
    return "VIOLATION_OBSERVED";
  }
  std::abort();
}

} // namespace blackbox
