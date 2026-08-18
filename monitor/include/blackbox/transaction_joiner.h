#pragma once

#include "blackbox/cdc_types.h"
#include "blackbox/checker_adapter.h"
#include "blackbox/client_txn.h"
#include "blackbox/monitor_error.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace blackbox {

enum class JoinLifecycle {
  AwaitRunStart,
  Bootstrap,
  Workload,
};

enum class HeadBlockReason {
  None,
  BootstrapValidation,
  MissingClient,
  EndFenceDeferred,
};

struct BootstrapPrefix {
  std::string run_id;
  std::uint64_t load_fence_cdc_seq = 0;
  std::uint64_t bootstrap_transactions = 0;

  bool operator==(const BootstrapPrefix &) const = default;
};

struct BootstrapTableCount {
  std::string table;
  std::uint64_t row_count = 0;

  bool operator==(const BootstrapTableCount &) const = default;
};

struct BootstrapStateProof {
  BootstrapPrefix prefix;
  std::vector<BootstrapTableCount> table_row_counts;
  std::string canonical_state_digest;

  bool operator==(const BootstrapStateProof &) const = default;
};

struct TransactionJoinerLimits {
  std::size_t max_pending_client_records = 1000000U;
  std::size_t max_pending_client_bytes = 256U * 1024U * 1024U;
  std::size_t max_pending_cdc_slots = 1000000U;
  std::size_t max_pending_cdc_bytes = 256U * 1024U * 1024U;
  std::size_t max_retained_identity_records = 10000000U;
  std::size_t max_retained_identity_bytes = 256U * 1024U * 1024U;
  std::size_t max_events_per_transaction = 101000U;
  std::size_t max_bootstrap_state_rows = 10000000U;
  std::size_t max_bootstrap_state_bytes = 256U * 1024U * 1024U;
  std::size_t max_checker_state_bytes = 512U * 1024U * 1024U;
};

struct TransactionJoinerConfig {
  std::string run_id;
  std::vector<std::string> expected_session_ids;
  std::vector<std::string> application_tables;
  ClientTxnParserLimits client_parser_limits;
  TransactionJoinerLimits limits;
};

struct TransactionJoinerMetrics {
  std::uint64_t next_cdc_seq = 0;
  std::uint64_t accepted_client_transactions = 0;
  std::uint64_t accepted_cdc_transactions = 0;
  std::uint64_t joined_workload_transactions = 0;
  std::uint64_t client_first_matches = 0;
  std::uint64_t cdc_first_matches = 0;
  std::size_t pending_client_records = 0;
  std::size_t pending_client_bytes = 0;
  std::size_t pending_client_high_water_bytes = 0;
  std::size_t pending_cdc_slots = 0;
  std::size_t pending_cdc_bytes = 0;
  std::size_t pending_cdc_high_water_bytes = 0;
  std::size_t retained_identity_records = 0;
  std::size_t retained_identity_bytes = 0;
  std::size_t bootstrap_state_rows = 0;
  std::size_t bootstrap_state_bytes = 0;
  bool end_fence_observed = false;
  CheckerAdapterMetrics checker;
};

class TransactionJoiner final {
public:
  explicit TransactionJoiner(TransactionJoinerConfig config);
  ~TransactionJoiner();

  TransactionJoiner(const TransactionJoiner &) = delete;
  TransactionJoiner &operator=(const TransactionJoiner &) = delete;
  TransactionJoiner(TransactionJoiner &&) noexcept;
  TransactionJoiner &operator=(TransactionJoiner &&) noexcept;

  void acceptClientPayload(std::string_view payload);
  void acceptCdcTransaction(CdcTransaction transaction);
  void validateBootstrapState(const BootstrapStateProof &proof);
  void drain();

  [[nodiscard]] std::optional<BootstrapPrefix> pendingBootstrapPrefix() const;
  [[nodiscard]] JoinLifecycle lifecycle() const noexcept;
  [[nodiscard]] HeadBlockReason headBlockReason() const noexcept;
  [[nodiscard]] CheckerObservation checkerObservation() const noexcept;
  [[nodiscard]] const std::optional<AttributedViolation> &
  firstViolation() const noexcept;
  [[nodiscard]] TransactionJoinerMetrics metrics() const;
  [[nodiscard]] bool failed() const noexcept;
  [[nodiscard]] std::optional<MonitorErrorCode> errorCode() const noexcept;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

[[nodiscard]] std::string_view toString(JoinLifecycle lifecycle) noexcept;
[[nodiscard]] std::string_view toString(HeadBlockReason reason) noexcept;

} // namespace blackbox
