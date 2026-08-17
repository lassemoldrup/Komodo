#pragma once

#include "blackbox/cdc_error.h"
#include "blackbox/cdc_types.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace blackbox {

struct CdcStreamRecord {
  std::string stream_id;
  std::string key_json;
  std::string envelope_json;
};

struct DebeziumAssemblerConfig {
  std::string run_id;
  std::string backend_source_id;
  std::string debezium_version = "3.6.1.Final";
  std::vector<std::string> application_tables;
  std::string transaction_marker_table;
  std::string run_fence_table;
  std::size_t max_active_transactions = 1024;
  std::size_t max_events_per_transaction = 100000;
  std::size_t max_active_bytes = 256U * 1024U * 1024U;
  std::size_t max_completed_transactions = 10000000;
  std::size_t max_retained_identity_bytes = 256U * 1024U * 1024U;
};

class DebeziumTxnAssembler final {
public:
  explicit DebeziumTxnAssembler(DebeziumAssemblerConfig config);
  ~DebeziumTxnAssembler();

  DebeziumTxnAssembler(const DebeziumTxnAssembler &) = delete;
  DebeziumTxnAssembler &operator=(const DebeziumTxnAssembler &) = delete;
  DebeziumTxnAssembler(DebeziumTxnAssembler &&) noexcept;
  DebeziumTxnAssembler &operator=(DebeziumTxnAssembler &&) noexcept;

  // Returns a transaction only for a fully validated END. Connector/server
  // transaction identifiers are retained solely inside the implementation to
  // validate Debezium structure; they are never exposed as correlation data.
  [[nodiscard]] std::optional<CdcTransaction>
  accept(const CdcStreamRecord &record);

  // Declare end-of-input. An active BEGIN without END is fatal. This does not
  // require lifecycle closure; END_FENCE reconciliation belongs to a later
  // milestone.
  void finish();

  [[nodiscard]] bool failed() const noexcept;
  [[nodiscard]] std::optional<CdcErrorCode> errorCode() const noexcept;
  [[nodiscard]] std::uint64_t completedCount() const noexcept;
  [[nodiscard]] std::size_t activeTransactionCount() const noexcept;
  [[nodiscard]] std::size_t activeBytes() const noexcept;
  [[nodiscard]] std::size_t retainedIdentityBytes() const noexcept;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace blackbox
