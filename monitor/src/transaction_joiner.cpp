#include "blackbox/transaction_joiner.h"

#include <openssl/evp.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <iomanip>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace blackbox {
namespace {

constexpr std::size_t node_charge = 64U;
constexpr std::size_t adapter_identity_charge = 128U;
constexpr std::size_t allocator_metadata_charge = 64U;
constexpr std::size_t minimum_allocator_size_class = 64U;
constexpr std::size_t allocator_page_size = 4096U;
constexpr std::uint64_t bootstrap_transaction_tag = 0x626f6f7473747270ULL;
constexpr std::uint64_t bootstrap_attempt_tag = 0x626f6f746174746dULL;
constexpr std::uint64_t phase_anchor_transaction_tag = 0x7068617365616e63ULL;
constexpr std::uint64_t phase_anchor_attempt_tag = 0x7068617365617474ULL;

struct CorrelationHash {
  [[nodiscard]] std::size_t
  operator()(const RunScopedCorrelationId &identity) const noexcept {
    auto result = std::hash<std::string>{}(identity.run_id);
    const auto session = std::hash<std::string>{}(identity.session_id);
    const auto transaction = std::hash<std::uint64_t>{}(identity.txn_id);
    result ^= session + 0x9e3779b97f4a7c15ULL + (result << 6U) + (result >> 2U);
    result ^=
        transaction + 0x9e3779b97f4a7c15ULL + (result << 6U) + (result >> 2U);
    return result;
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

struct BytesLess {
  [[nodiscard]] bool operator()(const CanonicalBytes &left,
                                const CanonicalBytes &right) const {
    return bytesLess(left, right);
  }
};

using BootstrapState = std::map<CanonicalBytes, CanonicalBytes, BytesLess>;

void checkedAdd(std::size_t &total, const std::size_t additional) {
  if (additional > std::numeric_limits<std::size_t>::max() - total) {
    throw MonitorError(MonitorErrorCode::ResourceExhausted,
                       "monitor byte accounting overflowed");
  }
  total += additional;
}

[[nodiscard]] std::size_t checkedProduct(const std::size_t count,
                                         const std::size_t width) {
  if (width != 0U && count > std::numeric_limits<std::size_t>::max() / width) {
    throw MonitorError(MonitorErrorCode::ResourceExhausted,
                       "monitor byte accounting overflowed");
  }
  return count * width;
}

// Charge small buffers as a full size class plus metadata. For glibc's large
// malloc/mmap path, include the allocator header and round the padded request
// to the next page. This intentionally overcounts SSO and keeps the byte budget
// conservative in the pinned Linux development/runtime environment.
[[nodiscard]] std::size_t allocationCharge(const std::size_t count,
                                           const std::size_t width = 1U) {
  if (count == 0U) {
    return 0U;
  }
  auto requested = checkedProduct(count, width);
  checkedAdd(requested, allocator_metadata_charge);
  if (requested >= allocator_page_size) {
    const auto remainder = requested % allocator_page_size;
    if (remainder != 0U) {
      checkedAdd(requested, allocator_page_size - remainder);
    }
    return requested;
  }
  std::size_t rounded = minimum_allocator_size_class;
  while (rounded < requested) {
    if (rounded > std::numeric_limits<std::size_t>::max() / 2U) {
      throw MonitorError(MonitorErrorCode::ResourceExhausted,
                         "monitor allocation accounting overflowed");
    }
    rounded *= 2U;
  }
  return rounded;
}

[[nodiscard]] std::size_t stringAllocationCharge(const std::string &value) {
  if (value.capacity() == std::numeric_limits<std::size_t>::max()) {
    throw MonitorError(MonitorErrorCode::ResourceExhausted,
                       "monitor string accounting overflowed");
  }
  return allocationCharge(value.capacity() + 1U);
}

[[nodiscard]] std::size_t clientCharge(const ClientTxn &client) {
  std::size_t result = sizeof(ClientTxn) + sizeof(std::size_t) + node_charge;
  checkedAdd(result, stringAllocationCharge(client.run_id));
  checkedAdd(result, stringAllocationCharge(client.session_id));
  checkedAdd(result, stringAllocationCharge(client.logical_txn_id));
  checkedAdd(result, stringAllocationCharge(client.attempt_id));
  checkedAdd(result, stringAllocationCharge(client.txn_type));
  checkedAdd(result, stringAllocationCharge(client.read_digest));
  checkedAdd(result, stringAllocationCharge(client.correlation_id.run_id));
  checkedAdd(result, stringAllocationCharge(client.correlation_id.session_id));
  checkedAdd(result, sizeof(RunScopedCorrelationId) + node_charge);
  checkedAdd(result, stringAllocationCharge(client.correlation_id.run_id));
  checkedAdd(result, stringAllocationCharge(client.correlation_id.session_id));
  checkedAdd(result,
             allocationCharge(client.reads.capacity(), sizeof(ClientRead)));
  for (const auto &read : client.reads) {
    checkedAdd(result, allocationCharge(read.key.capacity()));
    if (read.value.has_value()) {
      checkedAdd(result, allocationCharge(read.value->capacity()));
    }
  }
  return result;
}

[[nodiscard]] std::size_t cdcCharge(const CdcTransaction &transaction) {
  std::size_t result =
      sizeof(CdcTransaction) + sizeof(std::size_t) + node_charge;
  checkedAdd(result, stringAllocationCharge(transaction.begin_stream_id));
  checkedAdd(result, stringAllocationCharge(transaction.end_stream_id));
  checkedAdd(result, allocationCharge(transaction.writes.capacity(),
                                      sizeof(NormalizedWrite)));
  if (transaction.correlation_id.has_value()) {
    checkedAdd(result,
               stringAllocationCharge(transaction.correlation_id->run_id));
    checkedAdd(result,
               stringAllocationCharge(transaction.correlation_id->session_id));
    checkedAdd(result, sizeof(RunScopedCorrelationId) + node_charge);
    checkedAdd(result,
               stringAllocationCharge(transaction.correlation_id->run_id));
    checkedAdd(result,
               stringAllocationCharge(transaction.correlation_id->session_id));
  }
  for (const auto &write : transaction.writes) {
    checkedAdd(result, allocationCharge(write.key.capacity()));
    if (write.value.has_value()) {
      checkedAdd(result, allocationCharge(write.value->capacity()));
    }
  }
  return result;
}

[[nodiscard]] std::string syntheticUuid(const std::uint64_t tag,
                                        const std::uint64_t sequence) {
  std::ostringstream result;
  result << std::hex << std::setfill('0') << std::nouppercase << std::setw(8)
         << static_cast<std::uint32_t>(tag >> 32U) << '-' << std::setw(4)
         << static_cast<std::uint16_t>((tag >> 16U) & 0xffffU) << '-'
         << std::setw(4) << static_cast<std::uint16_t>(tag & 0xffffU) << '-'
         << std::setw(4) << static_cast<std::uint16_t>(sequence >> 48U) << '-'
         << std::setw(12) << (sequence & 0x0000ffffffffffffULL);
  return result.str();
}

class DigestContext final {
public:
  DigestContext()
      : context_(EVP_MD_CTX_new(),
                 [](EVP_MD_CTX *context) { EVP_MD_CTX_free(context); }) {
    if (!context_) {
      throw MonitorError(MonitorErrorCode::ResourceExhausted,
                         "could not allocate bootstrap digest context");
    }
    if (EVP_DigestInit_ex(context_.get(), EVP_sha256(), nullptr) != 1) {
      throw MonitorError(MonitorErrorCode::InternalError,
                         "could not initialize bootstrap digest");
    }
  }

  void update(const void *data, const std::size_t size) {
    if (size != 0U && EVP_DigestUpdate(context_.get(), data, size) != 1) {
      throw MonitorError(MonitorErrorCode::InternalError,
                         "could not update bootstrap digest");
    }
  }

  void update(const std::span<const std::byte> bytes) {
    update(bytes.data(), bytes.size());
  }

  [[nodiscard]] std::string finish() {
    std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
    unsigned int size = 0;
    if (EVP_DigestFinal_ex(context_.get(), digest.data(), &size) != 1 ||
        size != 32U) {
      throw MonitorError(MonitorErrorCode::InternalError,
                         "could not finish bootstrap digest");
    }
    constexpr std::string_view hex = "0123456789abcdef";
    std::string result(size * 2U, '0');
    for (std::size_t index = 0; index < size; ++index) {
      result[index * 2U] = hex[digest[index] >> 4U];
      result[index * 2U + 1U] = hex[digest[index] & 0x0fU];
    }
    return result;
  }

private:
  std::unique_ptr<EVP_MD_CTX, void (*)(EVP_MD_CTX *)> context_;
};

void updateU32(DigestContext &digest, const std::uint32_t value) {
  const std::array<unsigned char, 4> bytes = {
      static_cast<unsigned char>((value >> 24U) & 0xffU),
      static_cast<unsigned char>((value >> 16U) & 0xffU),
      static_cast<unsigned char>((value >> 8U) & 0xffU),
      static_cast<unsigned char>(value & 0xffU),
  };
  digest.update(bytes.data(), bytes.size());
}

void updateU64(DigestContext &digest, const std::uint64_t value) {
  const std::array<unsigned char, 8> bytes = {
      static_cast<unsigned char>((value >> 56U) & 0xffU),
      static_cast<unsigned char>((value >> 48U) & 0xffU),
      static_cast<unsigned char>((value >> 40U) & 0xffU),
      static_cast<unsigned char>((value >> 32U) & 0xffU),
      static_cast<unsigned char>((value >> 24U) & 0xffU),
      static_cast<unsigned char>((value >> 16U) & 0xffU),
      static_cast<unsigned char>((value >> 8U) & 0xffU),
      static_cast<unsigned char>(value & 0xffU),
  };
  digest.update(bytes.data(), bytes.size());
}

void updateLengthPrefixed(DigestContext &digest,
                          const std::span<const std::byte> bytes) {
  if (bytes.size() > std::numeric_limits<std::uint32_t>::max()) {
    throw MonitorError(MonitorErrorCode::ResourceExhausted,
                       "bootstrap digest LP payload exceeds uint32");
  }
  updateU32(digest, static_cast<std::uint32_t>(bytes.size()));
  digest.update(bytes);
}

[[nodiscard]] std::string bootstrapDigest(const BootstrapState &state) {
  constexpr std::array<char, 37> domain = {
      'd', 'b', '-', 'm', 'o', 'n', 'i', 't', 'o', 'r', 'i', 'n', 'g',
      '/', 'b', 'o', 'o', 't', 's', 't', 'r', 'a', 'p', '-', 's', 't',
      'a', 't', 'e', '-', 'd', 'i', 'g', 'e', 's', 't', '\0'};
  DigestContext digest;
  digest.update(domain.data(), domain.size());
  updateU64(digest, static_cast<std::uint64_t>(state.size()));
  for (const auto &[key, value] : state) {
    updateLengthPrefixed(digest, key);
    updateLengthPrefixed(digest, value);
  }
  return digest.finish();
}

[[nodiscard]] std::uint32_t readU32(const CanonicalBytes &bytes,
                                    std::size_t &cursor) {
  if (cursor > bytes.size() || bytes.size() - cursor < 4U) {
    throw MonitorError(MonitorErrorCode::InternalError,
                       "canonical bootstrap key has truncated framing");
  }
  const auto byte = [&bytes](const std::size_t index) {
    return std::to_integer<std::uint32_t>(bytes[index]);
  };
  const auto result = (byte(cursor) << 24U) | (byte(cursor + 1U) << 16U) |
                      (byte(cursor + 2U) << 8U) | byte(cursor + 3U);
  cursor += 4U;
  return result;
}

[[nodiscard]] std::string readKeyIdentifier(const CanonicalBytes &bytes,
                                            std::size_t &cursor) {
  const auto size = static_cast<std::size_t>(readU32(bytes, cursor));
  if (cursor > bytes.size() || size > bytes.size() - cursor) {
    throw MonitorError(MonitorErrorCode::InternalError,
                       "canonical bootstrap key has truncated identifier");
  }
  std::string result;
  result.reserve(size);
  for (std::size_t index = 0; index < size; ++index) {
    result.push_back(static_cast<char>(
        std::to_integer<unsigned char>(bytes[cursor + index])));
  }
  cursor += size;
  return result;
}

[[nodiscard]] std::string tableFromKey(const CanonicalBytes &key) {
  std::size_t cursor = 0;
  const auto schema = readKeyIdentifier(key, cursor);
  const auto table = readKeyIdentifier(key, cursor);
  if (cursor > key.size() || key.size() - cursor != 4U) {
    throw MonitorError(MonitorErrorCode::InternalError,
                       "canonical bootstrap key lacks its final I32");
  }
  return schema + "." + table;
}

[[nodiscard]] bool isSha256(const std::string_view value) {
  return value.size() == 64U &&
         std::all_of(value.begin(), value.end(), [](const char character) {
           return (character >= '0' && character <= '9') ||
                  (character >= 'a' && character <= 'f');
         });
}

} // namespace

class TransactionJoiner::Impl final {
public:
  explicit Impl(TransactionJoinerConfig config)
      : config_(std::move(config)),
        parser_(config_.run_id, config_.expected_session_ids,
                config_.application_tables, config_.client_parser_limits),
        checker_(CheckerAdapterConfig{
            .run_id = config_.run_id,
            .expected_session_ids = config_.expected_session_ids,
            .max_retained_state_bytes = config_.limits.max_checker_state_bytes,
            .max_events_per_transaction =
                config_.limits.max_events_per_transaction,
            .max_mapped_identity_records =
                config_.limits.max_retained_identity_records,
            .max_mapped_identity_bytes =
                config_.limits.max_retained_identity_bytes}) {
    validateConfig();
    expected_sessions_.insert(config_.expected_session_ids.begin(),
                              config_.expected_session_ids.end());
    application_tables_.insert(config_.application_tables.begin(),
                               config_.application_tables.end());
  }

  void acceptClientPayload(const std::string_view payload) {
    guarded([&] { acceptClient(parser_.parse(payload)); });
  }

  void acceptClientTransaction(ClientTxn transaction) {
    guarded([&] { acceptClient(std::move(transaction)); });
  }

  void acceptCdcTransaction(CdcTransaction transaction) {
    guarded([&] { acceptCdc(std::move(transaction)); });
  }

  void validateBootstrapState(const BootstrapStateProof &proof) {
    guarded([&] {
      const auto pending = pendingBootstrapPrefix();
      if (!pending.has_value()) {
        fail(MonitorErrorCode::FenceOrderInvalid,
             "bootstrap approval was supplied without a pending load fence");
      }
      if (proof.prefix != *pending) {
        fail(MonitorErrorCode::BootstrapStateMismatch,
             "bootstrap proof does not bind to the pending CDC prefix");
      }
      validateBootstrapProof(proof);
      approved_bootstrap_prefix_ = proof.prefix;
      bootstrap_state_validated_ = true;
      drainReadyPrefix();
    });
  }

  void processEndFence() {
    guarded([&] {
      drainReadyPrefix();
      if (cdc_slots_.empty() ||
          cdc_slots_.front().transaction.kind !=
              CdcTransactionKind::RunControl ||
          cdc_slots_.front().transaction.fence_phase !=
              RunFencePhase::EndFence) {
        fail(MonitorErrorCode::FenceOrderInvalid,
             "END_FENCE cannot pass an undrained CDC slot");
      }
      if (lifecycle_ != JoinLifecycle::Workload || end_fence_processed_) {
        fail(MonitorErrorCode::FenceOrderInvalid,
             "END_FENCE is not the next legal closure transition");
      }
      popCdcHead();
      lifecycle_ = JoinLifecycle::Closed;
      end_fence_processed_ = true;
      head_block_reason_ = HeadBlockReason::None;
    });
  }

  void drain() {
    guarded([&] { drainReadyPrefix(); });
  }

  [[nodiscard]] std::optional<BootstrapPrefix> pendingBootstrapPrefix() const {
    if (cdc_slots_.empty() ||
        cdc_slots_.front().transaction.kind != CdcTransactionKind::RunControl ||
        cdc_slots_.front().transaction.fence_phase !=
            RunFencePhase::LoadFinished ||
        lifecycle_ != JoinLifecycle::Bootstrap) {
      return std::nullopt;
    }
    return BootstrapPrefix{
        .run_id = config_.run_id,
        .load_fence_cdc_seq = cdc_slots_.front().transaction.cdc_seq,
        .bootstrap_transactions = bootstrap_transactions_,
    };
  }

  [[nodiscard]] JoinLifecycle lifecycle() const noexcept { return lifecycle_; }

  [[nodiscard]] HeadBlockReason headBlockReason() const noexcept {
    return head_block_reason_;
  }

  [[nodiscard]] CheckerObservation checkerObservation() const noexcept {
    return checker_.observation();
  }

  [[nodiscard]] const std::optional<AttributedViolation> &
  firstViolation() const noexcept {
    return checker_.firstViolation();
  }

  [[nodiscard]] TransactionJoinerMetrics metrics() const {
    return TransactionJoinerMetrics{
        .next_cdc_seq = next_cdc_seq_,
        .accepted_client_transactions = accepted_client_transactions_,
        .accepted_cdc_transactions = accepted_cdc_transactions_,
        .accepted_control_cdc_transactions = accepted_control_cdc_transactions_,
        .accepted_bootstrap_cdc_transactions =
            accepted_bootstrap_cdc_transactions_,
        .accepted_workload_cdc_transactions =
            accepted_workload_cdc_transactions_,
        .joined_workload_transactions = joined_workload_transactions_,
        .client_first_matches = client_first_matches_,
        .cdc_first_matches = cdc_first_matches_,
        .pending_client_records = client_by_correlation_.size(),
        .pending_client_bytes = pending_client_bytes_,
        .pending_client_high_water_bytes = pending_client_high_water_bytes_,
        .pending_cdc_slots = cdc_slots_.size(),
        .pending_cdc_bytes = pending_cdc_bytes_,
        .pending_cdc_high_water_bytes = pending_cdc_high_water_bytes_,
        .unmatched_client_records = unmatched_client_records_,
        .unmatched_workload_cdc_slots = unmatched_workload_cdc_slots_,
        .retained_identity_records = retained_identity_records_,
        .retained_identity_bytes = retained_identity_bytes_,
        .bootstrap_state_rows = bootstrap_state_.size(),
        .bootstrap_state_bytes = bootstrap_state_bytes_,
        .end_fence_observed = end_fence_observed_,
        .end_fence_processed = end_fence_processed_,
        .bootstrap_state_validated = bootstrap_state_validated_,
        .checker = checker_.metrics(),
    };
  }

  [[nodiscard]] TransactionJoinerReconciliationAudit
  auditReconciliationState() const {
    const auto audited_clients = unmatchedClientRecords();
    const auto audited_cdc = unmatchedWorkloadCdcSlots();
    return TransactionJoinerReconciliationAudit{
        .pending_client_records = client_by_correlation_.size(),
        .pending_cdc_slots = cdc_slots_.size(),
        .unmatched_client_records = audited_clients,
        .unmatched_workload_cdc_slots = audited_cdc,
        .incremental_counters_match =
            audited_clients == unmatched_client_records_ &&
            audited_cdc == unmatched_workload_cdc_slots_,
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
  struct BufferedClient {
    ClientTxn transaction;
    std::size_t charge = 0;
  };

  struct BufferedCdc {
    CdcTransaction transaction;
    std::size_t charge = 0;
  };

  struct LogicalMetadata {
    std::string session_id;
    std::uint64_t session_seq = 0;
    std::string attempt_id;
  };

  struct SessionProgress {
    std::uint64_t session_seq = 0;
    std::uint64_t attempt_surplus = 0;
  };

  struct Failure {
    MonitorErrorCode code;
    std::string message;
  };

  [[nodiscard]] std::size_t unmatchedClientRecords() const {
    return static_cast<std::size_t>(
        std::count_if(client_by_correlation_.begin(),
                      client_by_correlation_.end(), [this](const auto &entry) {
                        return !pending_cdc_correlations_.contains(entry.first);
                      }));
  }

  [[nodiscard]] std::size_t unmatchedWorkloadCdcSlots() const {
    return static_cast<std::size_t>(std::count_if(
        cdc_slots_.begin(), cdc_slots_.end(), [this](const BufferedCdc &slot) {
          return slot.transaction.kind == CdcTransactionKind::WorkloadMarked &&
                 !client_by_correlation_.contains(
                     *slot.transaction.correlation_id);
        }));
  }

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
                               "allocation failed while joining evidence");
      rememberFailure(error);
      throw error;
    } catch (const std::length_error &) {
      const MonitorError error(MonitorErrorCode::ResourceExhausted,
                               "container limit failed while joining evidence");
      rememberFailure(error);
      throw error;
    } catch (const std::exception &error) {
      const MonitorError wrapped(
          MonitorErrorCode::InternalError,
          std::string("unexpected transaction joiner failure: ") +
              error.what());
      rememberFailure(wrapped);
      throw wrapped;
    }
  }

  void validateConfig() const {
    const auto &limits = config_.limits;
    if (config_.expected_session_ids.empty() ||
        config_.application_tables.empty() ||
        limits.max_pending_client_records == 0U ||
        limits.max_pending_client_bytes == 0U ||
        limits.max_pending_cdc_slots == 0U ||
        limits.max_pending_cdc_bytes == 0U ||
        limits.max_retained_identity_records == 0U ||
        limits.max_retained_identity_bytes == 0U ||
        limits.max_events_per_transaction == 0U ||
        limits.max_bootstrap_state_rows == 0U ||
        limits.max_bootstrap_state_bytes == 0U ||
        limits.max_checker_state_bytes ==
            std::numeric_limits<std::size_t>::max()) {
      throw std::invalid_argument(
          "transaction joiner configuration is invalid");
    }
  }

  void requirePendingCapacity(const std::size_t count,
                              const std::size_t max_count,
                              const std::size_t bytes,
                              const std::size_t additional,
                              const std::size_t max_bytes,
                              const std::string_view label) const {
    if (count >= max_count || additional > max_bytes ||
        bytes > max_bytes - additional) {
      fail(MonitorErrorCode::JoinBufferLimitExceeded,
           std::string(label) + " exceeds its configured join-buffer limit");
    }
  }

  void reserveRetainedIdentities(const std::size_t records,
                                 const std::size_t bytes) {
    const auto &limits = config_.limits;
    if (records > limits.max_retained_identity_records ||
        retained_identity_records_ >
            limits.max_retained_identity_records - records ||
        bytes > limits.max_retained_identity_bytes ||
        retained_identity_bytes_ > limits.max_retained_identity_bytes - bytes) {
      fail(MonitorErrorCode::ResourceExhausted,
           "retained monitor identities exceed their configured limit");
    }
    retained_identity_records_ += records;
    retained_identity_bytes_ += bytes;
  }

  [[nodiscard]] std::size_t identityCharge(const std::string &value) const {
    std::size_t result = node_charge + sizeof(std::string);
    checkedAdd(result, stringAllocationCharge(value));
    return result;
  }

  void ensureExternalIdentityIsNotReserved(const ClientTxn &client) const {
    if (reserved_transaction_ids_.contains(client.logical_txn_id) ||
        reserved_attempt_ids_.contains(client.attempt_id)) {
      fail(MonitorErrorCode::InternalError,
           "external UUID collides with a reserved checker identity");
    }
  }

  void acceptClient(ClientTxn client) {
    ensureExternalIdentityIsNotReserved(client);
    if (seen_client_correlations_.contains(client.correlation_id)) {
      fail(MonitorErrorCode::ClientDuplicateCorrelation,
           "client marker identity is duplicated");
    }
    if (seen_attempt_ids_.contains(client.attempt_id)) {
      fail(MonitorErrorCode::AttemptDuplicate,
           "committed client attempt identity is duplicated");
    }
    const auto logical = logical_transactions_.find(client.logical_txn_id);
    if (logical != logical_transactions_.end()) {
      if (logical->second.session_id == client.session_id &&
          logical->second.session_seq == client.session_seq &&
          logical->second.attempt_id != client.attempt_id) {
        fail(MonitorErrorCode::MultipleCommittedAttempts,
             "one logical transaction has multiple committed attempts");
      }
      fail(MonitorErrorCode::LogicalTransactionDuplicate,
           "logical transaction identity is reused");
    }
    if (client.correlation_id.txn_id < client.session_seq) {
      fail(MonitorErrorCode::SessionAttemptProgressInvalid,
           "client marker txn_id is behind logical session progress");
    }
    const auto attempt_surplus =
        client.correlation_id.txn_id - client.session_seq;
    const auto session = client_session_progress_.find(client.session_id);
    if (session != client_session_progress_.end() &&
        client.session_seq <= session->second.session_seq) {
      fail(MonitorErrorCode::SessionSequenceNonIncreasing,
           "client publication session_seq is not increasing");
    }
    if (session != client_session_progress_.end() &&
        attempt_surplus < session->second.attempt_surplus) {
      fail(MonitorErrorCode::SessionAttemptProgressInvalid,
           "client retry surplus decreased within one session");
    }

    const auto charge = clientCharge(client);
    requirePendingCapacity(client_by_correlation_.size(),
                           config_.limits.max_pending_client_records,
                           pending_client_bytes_, charge,
                           config_.limits.max_pending_client_bytes,
                           "pending client evidence");

    std::size_t retained_records = 5U;
    std::size_t retained_bytes =
        node_charge + sizeof(std::string) + sizeof(LogicalMetadata);
    checkedAdd(retained_bytes, stringAllocationCharge(client.logical_txn_id));
    checkedAdd(retained_bytes, stringAllocationCharge(client.session_id));
    checkedAdd(retained_bytes, stringAllocationCharge(client.attempt_id));
    checkedAdd(retained_bytes, identityCharge(client.attempt_id));
    checkedAdd(retained_bytes, adapter_identity_charge * 2U);
    checkedAdd(retained_bytes, node_charge + sizeof(RunScopedCorrelationId));
    checkedAdd(retained_bytes,
               stringAllocationCharge(client.correlation_id.run_id));
    checkedAdd(retained_bytes,
               stringAllocationCharge(client.correlation_id.session_id));
    if (session == client_session_progress_.end()) {
      ++retained_records;
      checkedAdd(retained_bytes,
                 node_charge + sizeof(std::string) + sizeof(SessionProgress));
      checkedAdd(retained_bytes, stringAllocationCharge(client.session_id));
    }
    reserveRetainedIdentities(retained_records, retained_bytes);

    const bool cdc_is_pending =
        pending_cdc_correlations_.contains(client.correlation_id);
    if (cdc_is_pending) {
      ++cdc_first_matches_;
    }
    const auto correlation = client.correlation_id;
    const auto logical_id = client.logical_txn_id;
    const auto attempt_id = client.attempt_id;
    const auto session_id = client.session_id;
    const auto session_seq = client.session_seq;
    client_by_correlation_.emplace(
        correlation,
        BufferedClient{.transaction = std::move(client), .charge = charge});
    if (cdc_is_pending) {
      if (unmatched_workload_cdc_slots_ == 0U) {
        fail(MonitorErrorCode::InternalError,
             "unmatched CDC counter underflow while accepting client evidence");
      }
      --unmatched_workload_cdc_slots_;
    } else {
      ++unmatched_client_records_;
    }
    seen_client_correlations_.insert(correlation);
    seen_attempt_ids_.insert(attempt_id);
    logical_transactions_.emplace(logical_id,
                                  LogicalMetadata{.session_id = session_id,
                                                  .session_seq = session_seq,
                                                  .attempt_id = attempt_id});
    client_session_progress_[session_id] = SessionProgress{
        .session_seq = session_seq, .attempt_surplus = attempt_surplus};
    pending_client_bytes_ += charge;
    pending_client_high_water_bytes_ =
        std::max(pending_client_high_water_bytes_, pending_client_bytes_);
    ++accepted_client_transactions_;
    drainReadyPrefix();
  }

  void validateWrites(const std::vector<NormalizedWrite> &writes) const {
    for (std::size_t index = 0; index < writes.size(); ++index) {
      const auto &write = writes[index];
      if (write.key.empty()) {
        fail(MonitorErrorCode::CheckerInputRejected,
             "CDC normalized write has an empty key");
      }
      if (!application_tables_.contains(tableFromKey(write.key))) {
        fail(MonitorErrorCode::CheckerInputRejected,
             "CDC normalized write names an unallowlisted table");
      }
      switch (write.state) {
      case NormalizedValueState::Present:
        if (!write.value.has_value()) {
          fail(MonitorErrorCode::CheckerInputRejected,
               "present CDC write lacks a value");
        }
        break;
      case NormalizedValueState::Absent:
        if (write.value.has_value()) {
          fail(MonitorErrorCode::CheckerInputRejected,
               "absent CDC write carries a value");
        }
        break;
      default:
        fail(MonitorErrorCode::CheckerInputRejected,
             "CDC write has an invalid value state");
      }
      if (index != 0U && !bytesLess(writes[index - 1U].key, write.key)) {
        fail(MonitorErrorCode::CheckerInputRejected,
             "CDC writes are duplicated or reordered");
      }
    }
  }

  void validateCdc(const CdcTransaction &transaction) const {
    if (transaction.cdc_seq != next_cdc_seq_) {
      fail(MonitorErrorCode::CdcOrderInvalid,
           "completed CDC sequence is not zero-based and dense");
    }
    if (transaction.begin_stream_id.empty() ||
        transaction.end_stream_id.empty()) {
      fail(MonitorErrorCode::CdcOrderInvalid,
           "completed CDC transaction lacks stream boundaries");
    }
    validateWrites(transaction.writes);
    switch (transaction.kind) {
    case CdcTransactionKind::RunControl:
      if (!transaction.fence_phase.has_value() ||
          transaction.correlation_id.has_value() ||
          !transaction.writes.empty()) {
        fail(MonitorErrorCode::FenceOrderInvalid,
             "run-control CDC slot has an invalid shape");
      }
      break;
    case CdcTransactionKind::BootstrapApp:
      if (transaction.fence_phase.has_value() ||
          transaction.correlation_id.has_value()) {
        fail(MonitorErrorCode::CdcOrderInvalid,
             "bootstrap CDC slot has control or marker identity");
      }
      break;
    case CdcTransactionKind::WorkloadMarked:
      if (transaction.fence_phase.has_value() ||
          !transaction.correlation_id.has_value()) {
        fail(MonitorErrorCode::CorrelationMismatch,
             "workload CDC slot lacks exactly one marker identity");
      }
      if (transaction.correlation_id->run_id != config_.run_id) {
        fail(MonitorErrorCode::RunIdMismatch,
             "workload CDC slot belongs to another run");
      }
      if (!expected_sessions_.contains(
              transaction.correlation_id->session_id)) {
        fail(MonitorErrorCode::SessionUnknown,
             "workload CDC slot names an unknown session");
      }
      if (transaction.correlation_id->txn_id == 0U) {
        fail(MonitorErrorCode::CorrelationMismatch,
             "workload CDC slot has a zero marker txn_id");
      }
      break;
    default:
      fail(MonitorErrorCode::CdcOrderInvalid,
           "completed CDC transaction has an invalid kind");
    }
  }

  void acceptCdc(CdcTransaction transaction) {
    validateCdc(transaction);
    if (transaction.kind == CdcTransactionKind::WorkloadMarked &&
        seen_cdc_correlations_.contains(*transaction.correlation_id)) {
      fail(MonitorErrorCode::MarkerDuplicate,
           "CDC marker identity is duplicated");
    }
    if (end_fence_observed_) {
      fail(MonitorErrorCode::CdcOrderInvalid,
           "completed CDC transaction follows END_FENCE");
    }
    if (next_cdc_seq_ == std::numeric_limits<std::uint64_t>::max()) {
      fail(MonitorErrorCode::ResourceExhausted,
           "completed CDC sequence space is exhausted");
    }
    const auto charge = cdcCharge(transaction);
    requirePendingCapacity(
        cdc_slots_.size(), config_.limits.max_pending_cdc_slots,
        pending_cdc_bytes_, charge, config_.limits.max_pending_cdc_bytes,
        "pending CDC evidence");

    std::optional<RunScopedCorrelationId> workload_correlation;
    bool client_is_pending = false;
    if (transaction.kind == CdcTransactionKind::WorkloadMarked) {
      workload_correlation = *transaction.correlation_id;
      const auto &correlation = *workload_correlation;
      std::size_t identity_bytes = node_charge + sizeof(RunScopedCorrelationId);
      checkedAdd(identity_bytes, stringAllocationCharge(correlation.run_id));
      checkedAdd(identity_bytes,
                 stringAllocationCharge(correlation.session_id));
      reserveRetainedIdentities(1U, identity_bytes);
      client_is_pending = client_by_correlation_.contains(correlation);
    }
    const auto kind = transaction.kind;
    const bool is_end_fence =
        transaction.fence_phase == RunFencePhase::EndFence;
    cdc_slots_.push_back(
        BufferedCdc{.transaction = std::move(transaction), .charge = charge});
    if (workload_correlation.has_value()) {
      if (client_is_pending) {
        if (unmatched_client_records_ == 0U) {
          fail(MonitorErrorCode::InternalError,
               "unmatched client counter underflow while accepting CDC "
               "evidence");
        }
        --unmatched_client_records_;
        ++client_first_matches_;
      } else {
        ++unmatched_workload_cdc_slots_;
      }
      seen_cdc_correlations_.insert(*workload_correlation);
      pending_cdc_correlations_.insert(*workload_correlation);
    }
    if (is_end_fence) {
      end_fence_observed_ = true;
    }
    pending_cdc_bytes_ += charge;
    pending_cdc_high_water_bytes_ =
        std::max(pending_cdc_high_water_bytes_, pending_cdc_bytes_);
    ++next_cdc_seq_;
    ++accepted_cdc_transactions_;
    switch (kind) {
    case CdcTransactionKind::RunControl:
      ++accepted_control_cdc_transactions_;
      break;
    case CdcTransactionKind::BootstrapApp:
      ++accepted_bootstrap_cdc_transactions_;
      break;
    case CdcTransactionKind::WorkloadMarked:
      ++accepted_workload_cdc_transactions_;
      break;
    }
    drainReadyPrefix();
  }

  [[nodiscard]] std::size_t
  bootstrapEntryCharge(const CanonicalBytes &key,
                       const CanonicalBytes &value) const {
    std::size_t result = sizeof(BootstrapState::value_type) + node_charge;
    checkedAdd(result, allocationCharge(key.capacity()));
    checkedAdd(result, allocationCharge(value.capacity()));
    return result;
  }

  void requireBootstrapCapacity(const std::size_t rows,
                                const std::size_t bytes) const {
    if (rows > config_.limits.max_bootstrap_state_rows ||
        bytes > config_.limits.max_bootstrap_state_bytes) {
      fail(MonitorErrorCode::ResourceExhausted,
           "reconstructed bootstrap state exceeds its configured limit");
    }
  }

  void applyBootstrapWrites(const std::vector<NormalizedWrite> &writes) {
    for (const auto &write : writes) {
      const auto found = bootstrap_state_.find(write.key);
      if (write.state == NormalizedValueState::Absent) {
        if (found != bootstrap_state_.end()) {
          bootstrap_state_bytes_ -=
              bootstrapEntryCharge(found->first, found->second);
          bootstrap_state_.erase(found);
        }
        continue;
      }

      if (found == bootstrap_state_.end()) {
        std::size_t prospective = bootstrap_state_bytes_;
        checkedAdd(prospective, bootstrapEntryCharge(write.key, *write.value));
        requireBootstrapCapacity(bootstrap_state_.size() + 1U, prospective);
        const auto [inserted, unused] =
            bootstrap_state_.emplace(write.key, *write.value);
        (void)unused;
        const auto actual =
            bootstrapEntryCharge(inserted->first, inserted->second);
        checkedAdd(bootstrap_state_bytes_, actual);
        requireBootstrapCapacity(bootstrap_state_.size(),
                                 bootstrap_state_bytes_);
        continue;
      }

      const auto old_charge = bootstrapEntryCharge(found->first, found->second);
      std::size_t prospective = bootstrap_state_bytes_ - old_charge;
      checkedAdd(prospective, bootstrapEntryCharge(found->first, *write.value));
      requireBootstrapCapacity(bootstrap_state_.size(), prospective);
      found->second = *write.value;
      bootstrap_state_bytes_ -= old_charge;
      checkedAdd(bootstrap_state_bytes_,
                 bootstrapEntryCharge(found->first, found->second));
      requireBootstrapCapacity(bootstrap_state_.size(), bootstrap_state_bytes_);
    }
  }

  void validateBootstrapProof(const BootstrapStateProof &proof) const {
    if (!isSha256(proof.canonical_state_digest)) {
      fail(MonitorErrorCode::BootstrapStateMismatch,
           "bootstrap proof digest is not lowercase SHA-256 hex");
    }
    if (proof.table_row_counts.size() != config_.application_tables.size()) {
      fail(MonitorErrorCode::BootstrapStateMismatch,
           "bootstrap proof does not cover every application table");
    }

    std::map<std::string, std::uint64_t> declared_counts;
    std::uint64_t declared_total = 0;
    for (const auto &entry : proof.table_row_counts) {
      if (!application_tables_.contains(entry.table) ||
          !declared_counts.emplace(entry.table, entry.row_count).second ||
          entry.row_count >
              std::numeric_limits<std::uint64_t>::max() - declared_total) {
        fail(MonitorErrorCode::BootstrapStateMismatch,
             "bootstrap proof table counts are unknown, duplicated, or "
             "overflow");
      }
      declared_total += entry.row_count;
    }
    if (declared_total != bootstrap_state_.size()) {
      fail(MonitorErrorCode::BootstrapStateMismatch,
           "bootstrap proof row-count sum differs from reconstructed state");
    }

    std::map<std::string, std::uint64_t> actual_counts;
    for (const auto &table : config_.application_tables) {
      actual_counts.emplace(table, 0U);
    }
    for (const auto &[key, unused] : bootstrap_state_) {
      (void)unused;
      const auto table = tableFromKey(key);
      const auto found = actual_counts.find(table);
      if (found == actual_counts.end() ||
          found->second == std::numeric_limits<std::uint64_t>::max()) {
        fail(MonitorErrorCode::InternalError,
             "reconstructed bootstrap state has an invalid table count");
      }
      ++found->second;
    }
    if (declared_counts != actual_counts ||
        proof.canonical_state_digest != bootstrapDigest(bootstrap_state_)) {
      fail(MonitorErrorCode::BootstrapStateMismatch,
           "bootstrap proof differs from reconstructed CDC state");
    }
  }

  void reserveSyntheticIdentity(const std::string &transaction_id,
                                const std::string &attempt_id) {
    if (logical_transactions_.contains(transaction_id) ||
        seen_attempt_ids_.contains(attempt_id)) {
      fail(MonitorErrorCode::InternalError,
           "reserved checker identity collides with client UUID evidence");
    }
    std::size_t bytes = 0;
    checkedAdd(bytes, identityCharge(transaction_id));
    checkedAdd(bytes, identityCharge(attempt_id));
    checkedAdd(bytes, adapter_identity_charge * 2U);
    reserveRetainedIdentities(4U, bytes);
    reserved_transaction_ids_.insert(transaction_id);
    reserved_attempt_ids_.insert(attempt_id);
  }

  void preflightAnchors() {
    std::vector<std::pair<std::string, std::string>> identities;
    identities.reserve(config_.expected_session_ids.size());
    std::size_t retained_bytes = 0;
    for (std::size_t index = 0; index < config_.expected_session_ids.size();
         ++index) {
      const auto ordinal = static_cast<std::uint64_t>(index + 1U);
      auto transaction_id =
          syntheticUuid(phase_anchor_transaction_tag, ordinal);
      auto attempt_id = syntheticUuid(phase_anchor_attempt_tag, ordinal);
      if (logical_transactions_.contains(transaction_id) ||
          seen_attempt_ids_.contains(attempt_id)) {
        fail(MonitorErrorCode::InternalError,
             "phase-anchor identity collides with client UUID evidence");
      }
      checkedAdd(retained_bytes, identityCharge(transaction_id));
      checkedAdd(retained_bytes, identityCharge(attempt_id));
      checkedAdd(retained_bytes, adapter_identity_charge * 2U);
      identities.emplace_back(std::move(transaction_id), std::move(attempt_id));
    }
    reserveRetainedIdentities(
        checkedProduct(identities.size(), static_cast<std::size_t>(4U)),
        retained_bytes);
    for (const auto &[transaction_id, attempt_id] : identities) {
      reserved_transaction_ids_.insert(transaction_id);
      reserved_attempt_ids_.insert(attempt_id);
    }
  }

  void popCdcHead() {
    pending_cdc_bytes_ -= cdc_slots_.front().charge;
    cdc_slots_.pop_front();
  }

  void drainReadyPrefix() {
    head_block_reason_ = HeadBlockReason::None;
    while (!cdc_slots_.empty()) {
      auto &head = cdc_slots_.front().transaction;
      if (head.kind == CdcTransactionKind::RunControl) {
        switch (*head.fence_phase) {
        case RunFencePhase::RunStart:
          if (lifecycle_ != JoinLifecycle::AwaitRunStart) {
            fail(MonitorErrorCode::FenceOrderInvalid,
                 "RUN_START is not the next legal prefix transition");
          }
          lifecycle_ = JoinLifecycle::Bootstrap;
          popCdcHead();
          continue;
        case RunFencePhase::LoadFinished: {
          if (lifecycle_ != JoinLifecycle::Bootstrap) {
            fail(MonitorErrorCode::FenceOrderInvalid,
                 "LOAD_FINISHED is not the next legal prefix transition");
          }
          const auto pending = pendingBootstrapPrefix();
          if (!pending.has_value() || approved_bootstrap_prefix_ != pending) {
            head_block_reason_ = HeadBlockReason::BootstrapValidation;
            return;
          }
          preflightAnchors();
          for (const auto &session_id : config_.expected_session_ids) {
            checker_.consumePhaseAnchor(next_checker_commit_seq_, session_id);
            ++next_checker_commit_seq_;
            ++phase_anchor_transactions_;
          }
          lifecycle_ = JoinLifecycle::Workload;
          popCdcHead();
          continue;
        }
        case RunFencePhase::EndFence:
          if (lifecycle_ != JoinLifecycle::Workload) {
            fail(MonitorErrorCode::FenceOrderInvalid,
                 "END_FENCE reached an illegal drained prefix");
          }
          head_block_reason_ = HeadBlockReason::EndFenceDeferred;
          return;
        }
      }

      if (head.kind == CdcTransactionKind::BootstrapApp) {
        if (lifecycle_ != JoinLifecycle::Bootstrap) {
          fail(MonitorErrorCode::FenceOrderInvalid,
               "bootstrap CDC transaction is outside the load prefix");
        }
        const auto transaction_id =
            syntheticUuid(bootstrap_transaction_tag, head.cdc_seq);
        const auto attempt_id =
            syntheticUuid(bootstrap_attempt_tag, head.cdc_seq);
        reserveSyntheticIdentity(transaction_id, attempt_id);
        checker_.consumeBootstrap(next_checker_commit_seq_, head.cdc_seq,
                                  head.writes);
        applyBootstrapWrites(head.writes);
        ++next_checker_commit_seq_;
        ++bootstrap_transactions_;
        popCdcHead();
        continue;
      }

      if (head.kind != CdcTransactionKind::WorkloadMarked ||
          lifecycle_ != JoinLifecycle::Workload) {
        fail(MonitorErrorCode::FenceOrderInvalid,
             "workload CDC transaction is outside the workload prefix");
      }
      const auto correlation = *head.correlation_id;
      const auto client = client_by_correlation_.find(correlation);
      if (client == client_by_correlation_.end()) {
        head_block_reason_ = HeadBlockReason::MissingClient;
        return;
      }
      checker_.consumeWorkload(next_checker_commit_seq_,
                               client->second.transaction, head.writes);
      ++next_checker_commit_seq_;
      ++joined_workload_transactions_;
      pending_client_bytes_ -= client->second.charge;
      client_by_correlation_.erase(client);
      pending_cdc_correlations_.erase(correlation);
      popCdcHead();
    }
  }

  TransactionJoinerConfig config_;
  ClientTxnParser parser_;
  CheckerAdapter checker_;
  std::unordered_set<std::string> expected_sessions_;
  std::unordered_set<std::string> application_tables_;
  BootstrapState bootstrap_state_;
  std::unordered_map<RunScopedCorrelationId, BufferedClient, CorrelationHash>
      client_by_correlation_;
  std::deque<BufferedCdc> cdc_slots_;
  std::unordered_set<RunScopedCorrelationId, CorrelationHash>
      seen_client_correlations_;
  std::unordered_set<RunScopedCorrelationId, CorrelationHash>
      seen_cdc_correlations_;
  std::unordered_set<RunScopedCorrelationId, CorrelationHash>
      pending_cdc_correlations_;
  std::unordered_set<std::string> seen_attempt_ids_;
  std::unordered_map<std::string, LogicalMetadata> logical_transactions_;
  std::unordered_map<std::string, SessionProgress> client_session_progress_;
  std::unordered_set<std::string> reserved_transaction_ids_;
  std::unordered_set<std::string> reserved_attempt_ids_;
  std::optional<BootstrapPrefix> approved_bootstrap_prefix_;
  std::optional<Failure> failure_;
  JoinLifecycle lifecycle_ = JoinLifecycle::AwaitRunStart;
  HeadBlockReason head_block_reason_ = HeadBlockReason::None;
  std::uint64_t next_cdc_seq_ = 0;
  std::uint64_t next_checker_commit_seq_ = 0;
  std::uint64_t accepted_client_transactions_ = 0;
  std::uint64_t accepted_cdc_transactions_ = 0;
  std::uint64_t accepted_control_cdc_transactions_ = 0;
  std::uint64_t accepted_bootstrap_cdc_transactions_ = 0;
  std::uint64_t accepted_workload_cdc_transactions_ = 0;
  std::uint64_t joined_workload_transactions_ = 0;
  std::uint64_t bootstrap_transactions_ = 0;
  std::uint64_t phase_anchor_transactions_ = 0;
  std::uint64_t client_first_matches_ = 0;
  std::uint64_t cdc_first_matches_ = 0;
  std::size_t pending_client_bytes_ = 0;
  std::size_t pending_client_high_water_bytes_ = 0;
  std::size_t pending_cdc_bytes_ = 0;
  std::size_t pending_cdc_high_water_bytes_ = 0;
  std::size_t unmatched_client_records_ = 0;
  std::size_t unmatched_workload_cdc_slots_ = 0;
  std::size_t retained_identity_records_ = 0;
  std::size_t retained_identity_bytes_ = 0;
  std::size_t bootstrap_state_bytes_ = 0;
  bool end_fence_observed_ = false;
  bool end_fence_processed_ = false;
  bool bootstrap_state_validated_ = false;
};

TransactionJoiner::TransactionJoiner(TransactionJoinerConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

TransactionJoiner::~TransactionJoiner() = default;
TransactionJoiner::TransactionJoiner(TransactionJoiner &&) noexcept = default;
TransactionJoiner &
TransactionJoiner::operator=(TransactionJoiner &&) noexcept = default;

void TransactionJoiner::acceptClientPayload(const std::string_view payload) {
  impl_->acceptClientPayload(payload);
}

void TransactionJoiner::acceptClientTransaction(ClientTxn transaction) {
  impl_->acceptClientTransaction(std::move(transaction));
}

void TransactionJoiner::acceptCdcTransaction(CdcTransaction transaction) {
  impl_->acceptCdcTransaction(std::move(transaction));
}

void TransactionJoiner::validateBootstrapState(
    const BootstrapStateProof &proof) {
  impl_->validateBootstrapState(proof);
}

void TransactionJoiner::processEndFence() { impl_->processEndFence(); }

void TransactionJoiner::drain() { impl_->drain(); }

std::optional<BootstrapPrefix>
TransactionJoiner::pendingBootstrapPrefix() const {
  return impl_->pendingBootstrapPrefix();
}

JoinLifecycle TransactionJoiner::lifecycle() const noexcept {
  return impl_->lifecycle();
}

HeadBlockReason TransactionJoiner::headBlockReason() const noexcept {
  return impl_->headBlockReason();
}

CheckerObservation TransactionJoiner::checkerObservation() const noexcept {
  return impl_->checkerObservation();
}

const std::optional<AttributedViolation> &
TransactionJoiner::firstViolation() const noexcept {
  return impl_->firstViolation();
}

TransactionJoinerMetrics TransactionJoiner::metrics() const {
  return impl_->metrics();
}

TransactionJoinerReconciliationAudit
TransactionJoiner::auditReconciliationState() const {
  return impl_->auditReconciliationState();
}

bool TransactionJoiner::failed() const noexcept { return impl_->failed(); }

std::optional<MonitorErrorCode> TransactionJoiner::errorCode() const noexcept {
  return impl_->errorCode();
}

std::string_view toString(const JoinLifecycle lifecycle) noexcept {
  switch (lifecycle) {
  case JoinLifecycle::AwaitRunStart:
    return "AWAIT_RUN_START";
  case JoinLifecycle::Bootstrap:
    return "BOOTSTRAP";
  case JoinLifecycle::Workload:
    return "WORKLOAD";
  case JoinLifecycle::Closed:
    return "CLOSED";
  }
  std::abort();
}

std::string_view toString(const HeadBlockReason reason) noexcept {
  switch (reason) {
  case HeadBlockReason::None:
    return "NONE";
  case HeadBlockReason::BootstrapValidation:
    return "BOOTSTRAP_VALIDATION";
  case HeadBlockReason::MissingClient:
    return "MISSING_CLIENT";
  case HeadBlockReason::EndFenceDeferred:
    return "END_FENCE_DEFERRED";
  }
  std::abort();
}

} // namespace blackbox
