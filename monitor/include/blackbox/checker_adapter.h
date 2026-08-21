#pragma once

#include "blackbox/cdc_types.h"
#include "blackbox/client_txn.h"
#include "blackbox/monitor_error.h"

#include "co_checker/online/engine.h"
#include "co_checker/online/result.h"
#include "co_checker/online/types.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace blackbox {

enum class CheckerObservation {
  NotStarted,
  PassSoFar,
  ViolationObserved,
};

struct CheckerTransactionIdentity {
  co_checker::online::TransactionUid transaction_id;
  co_checker::online::TransactionAttemptUid attempt_id;
  co_checker::online::SessionUid session_id;
  std::uint64_t session_seq = 0;

  bool operator==(const CheckerTransactionIdentity &) const = default;
};

struct AttributedViolation {
  std::string run_id;
  std::string session_id;
  std::string logical_txn_id;
  std::string attempt_id;
  RunScopedCorrelationId correlation_id;
  std::uint64_t session_seq = 0;
  co_checker::online::Violation checker_violation;

  bool operator==(const AttributedViolation &) const = default;
};

struct CheckerAdapterConfig {
  std::string run_id;
  std::vector<std::string> expected_session_ids;
  std::size_t max_retained_state_bytes = 512U * 1024U * 1024U;
  std::size_t max_events_per_transaction = 101000U;
  std::size_t max_mapped_identity_records = 10000000U;
  std::size_t max_mapped_identity_bytes = 256U * 1024U * 1024U;
  std::function<std::uint64_t()> monotonic_clock_nanoseconds = {};
};

struct CheckerAdapterMetrics {
  std::uint64_t next_commit_seq = 0;
  std::uint64_t consumed_transactions = 0;
  std::uint64_t bootstrap_transactions = 0;
  std::uint64_t phase_anchor_transactions = 0;
  std::uint64_t workload_transactions = 0;
  std::uint64_t read_events = 0;
  std::uint64_t write_events = 0;
  std::uint64_t consume_total_nanoseconds = 0;
  std::uint64_t consume_max_nanoseconds = 0;
  // Workload-only counters and time spent inside the checker engine consume
  // call. Bootstrap and phase-anchor transactions are excluded.
  std::uint64_t workload_read_events = 0;
  std::uint64_t workload_write_events = 0;
  std::uint64_t workload_consume_total_nanoseconds = 0;
  std::uint64_t workload_consume_max_nanoseconds = 0;
  std::size_t mapped_identity_records = 0;
  std::size_t mapped_identity_bytes = 0;
  std::size_t mapped_identity_high_water_records = 0;
  std::size_t mapped_identity_high_water_bytes = 0;
  std::size_t events_per_transaction_high_water = 0;
  co_checker::online::SnapshotSOWWMetrics engine;
};

[[nodiscard]] CheckerTransactionIdentity
makeBootstrapCheckerIdentity(std::string_view run_id, std::uint64_t cdc_seq);

[[nodiscard]] CheckerTransactionIdentity
makePhaseAnchorCheckerIdentity(std::string_view run_id,
                               std::uint32_t session_ordinal);

class CheckerAdapter final {
public:
  explicit CheckerAdapter(CheckerAdapterConfig config);
  ~CheckerAdapter();

  CheckerAdapter(const CheckerAdapter &) = delete;
  CheckerAdapter &operator=(const CheckerAdapter &) = delete;
  CheckerAdapter(CheckerAdapter &&) noexcept;
  CheckerAdapter &operator=(CheckerAdapter &&) noexcept;

  void consumeBootstrap(std::uint64_t commit_seq, std::uint64_t cdc_seq,
                        const std::vector<NormalizedWrite> &writes);
  void consumePhaseAnchor(std::uint64_t commit_seq,
                          std::string_view session_id);
  void consumeWorkload(std::uint64_t commit_seq, const ClientTxn &client,
                       const std::vector<NormalizedWrite> &writes);

  [[nodiscard]] CheckerObservation observation() const noexcept;
  [[nodiscard]] const std::optional<AttributedViolation> &
  firstViolation() const noexcept;
  [[nodiscard]] CheckerAdapterMetrics metrics() const;
  [[nodiscard]] bool failed() const noexcept;
  [[nodiscard]] std::optional<MonitorErrorCode> errorCode() const noexcept;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

[[nodiscard]] std::string_view
toString(CheckerObservation observation) noexcept;

} // namespace blackbox
