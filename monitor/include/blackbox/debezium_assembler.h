#pragma once

#include "blackbox/backend_adapter.h"
#include "blackbox/benchmark_adapter.h"
#include "blackbox/cdc_error.h"
#include "blackbox/cdc_types.h"

#include <cstddef>
#include <cstdint>
#include <functional>
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
  const BackendAdapter *backend_adapter = nullptr;
  const BenchmarkAdapter *benchmark_adapter = nullptr;
  std::vector<std::string> application_tables;
  std::string transaction_marker_table;
  std::string run_fence_table;
  std::size_t max_active_transactions = 1024;
  std::size_t max_events_per_transaction = 100000;
  std::size_t max_active_bytes = 256U * 1024U * 1024U;
  std::size_t max_completed_transactions = 10000000;
  std::size_t max_retained_identity_bytes = 256U * 1024U * 1024U;
  std::function<std::uint64_t()> monotonic_clock_nanoseconds = {};
};

struct DebeziumAssemblerMetrics {
  std::uint64_t accepted_raw_records = 0;
  std::uint64_t accepted_raw_bytes = 0;
  std::uint64_t startup_schema_records = 0;
  std::uint64_t begin_metadata_records = 0;
  std::uint64_t end_metadata_records = 0;
  std::uint64_t row_records = 0;
  std::uint64_t completed_transactions = 0;
  std::uint64_t completed_run_control_transactions = 0;
  std::uint64_t completed_bootstrap_transactions = 0;
  std::uint64_t completed_workload_transactions = 0;
  std::uint64_t raw_row_events = 0;
  std::uint64_t normalized_write_events = 0;
  std::uint64_t workload_normalized_write_events = 0;
  // Successful accept() calls only. Workload processing aggregates the
  // BEGIN, row, and END calls for transactions ultimately classified as
  // WorkloadMarked; its maximum is per completed workload transaction.
  std::uint64_t accept_total_nanoseconds = 0;
  std::uint64_t accept_max_nanoseconds = 0;
  std::uint64_t workload_processing_total_nanoseconds = 0;
  std::uint64_t workload_processing_max_nanoseconds = 0;
  std::size_t active_transactions = 0;
  std::size_t active_transactions_high_water = 0;
  std::size_t observed_events_per_transaction_high_water = 0;
  std::size_t active_assembly_bytes = 0;
  std::size_t active_assembly_high_water_bytes = 0;
  std::size_t retained_identity_records = 0;
  std::size_t retained_identity_bytes = 0;
  std::size_t retained_identity_high_water_records = 0;
  std::size_t retained_identity_high_water_bytes = 0;
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

  // Declare end-of-input. An active BEGIN without END is fatal. Run lifecycle
  // closure and END_FENCE reconciliation remain the controller's
  // responsibility.
  void finish();

  [[nodiscard]] bool failed() const noexcept;
  [[nodiscard]] std::optional<CdcErrorCode> errorCode() const noexcept;
  [[nodiscard]] std::uint64_t completedCount() const noexcept;
  [[nodiscard]] std::size_t activeTransactionCount() const noexcept;
  [[nodiscard]] std::size_t activeBytes() const noexcept;
  [[nodiscard]] std::size_t retainedIdentityBytes() const noexcept;
  [[nodiscard]] DebeziumAssemblerMetrics metrics() const noexcept;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace blackbox
