#pragma once

#include "blackbox/cdc_types.h"
#include "blackbox/monitor_error.h"
#include "blackbox/protocol_records.h"
#include "blackbox/transaction_joiner.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace blackbox {

enum class DiagnosticCategory {
  Checker,
  Protocol,
  Operational,
};

enum class FinalOutcome {
  Pass,
  Violation,
  Invalid,
};

struct RunDiagnostic {
  DiagnosticCategory category = DiagnosticCategory::Protocol;
  std::string code;
  std::string message;

  bool operator==(const RunDiagnostic &) const = default;
};

struct ReconciliationSummary {
  bool evidence_reconciled = false;
  bool bootstrap_manifest_validated = false;
  bool end_fence_seen = false;
  std::uint64_t expected_sessions = 0;
  std::uint64_t completed_sessions = 0;
  std::uint64_t accepted_client_transactions = 0;
  std::uint64_t joined_workload_transactions = 0;
  std::uint64_t bootstrap_cdc_transactions = 0;
  std::uint64_t bootstrap_checker_transactions = 0;
  std::uint64_t phase_anchor_transactions = 0;
  std::uint64_t checker_consumed_transactions = 0;
  std::uint64_t unmatched_client_records = 0;
  std::uint64_t unmatched_workload_cdc_slots = 0;
  std::uint64_t ambiguous_attempts = 0;

  bool operator==(const ReconciliationSummary &) const = default;
};

struct RunControllerLimits {
  std::size_t max_diagnostics = 64U;
  std::size_t max_pending_status_records = 32U;
  std::size_t max_diagnostic_message_bytes = 4096U;
};

struct RunControllerConfig {
  ClientTxnParserLimits client_parser_limits;
  ProtocolRecordParserLimits record_parser_limits;
  TransactionJoinerLimits joiner_limits;
  RunControllerLimits limits;
};

// Owns one complete fenced run. The caller supplies assembled CDC transactions
// in source order and client stream payloads in their Redis stream order. An
// input-idle notification is a batch-boundary signal; it becomes terminal only
// after END_FENCE has been observed.
class RunController final {
public:
  explicit RunController(RunManifest manifest, RunControllerConfig config = {});
  ~RunController();

  RunController(const RunController &) = delete;
  RunController &operator=(const RunController &) = delete;
  RunController(RunController &&) noexcept;
  RunController &operator=(RunController &&) noexcept;

  void setLastCdcStreamId(std::string stream_id);
  void setLastClientStreamId(std::string stream_id);

  void acceptClientPayload(std::string_view payload);
  void acceptCdcTransaction(CdcTransaction transaction);

  void noteExternalFailure(DiagnosticCategory category, std::string code,
                           std::string message);
  void noteExternalFailure(std::string_view category, std::string code,
                           std::string message);

  void onInputIdle();
  void forceTimeoutFinal();
  void forceInvalidFinal(MonitorErrorCode code, std::string message);
  void forceInvalidFinal(std::string code, std::string message);

  [[nodiscard]] bool terminal() const noexcept;
  [[nodiscard]] bool endFenceObserved() const noexcept;
  [[nodiscard]] bool endFenceProcessed() const noexcept;
  [[nodiscard]] std::optional<FinalOutcome> finalOutcome() const noexcept;
  [[nodiscard]] const std::optional<ReconciliationSummary> &
  finalReconciliation() const noexcept;
  [[nodiscard]] const std::vector<RunDiagnostic> &diagnostics() const noexcept;
  [[nodiscard]] TransactionJoinerMetrics joinerMetrics() const;

  [[nodiscard]] std::vector<std::string> takePendingStatuses();
  [[nodiscard]] std::string finalReport() const;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

[[nodiscard]] std::string_view toString(DiagnosticCategory category) noexcept;
[[nodiscard]] std::string_view toString(FinalOutcome outcome) noexcept;

} // namespace blackbox
