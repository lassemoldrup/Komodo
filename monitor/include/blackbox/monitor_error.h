#pragma once

#include <stdexcept>
#include <string>
#include <string_view>

namespace blackbox {

enum class MonitorErrorCode {
  UnsupportedTransaction,
  UnsupportedReadSource,
  CanonicalEncodingFailed,
  ClientRecordMalformed,
  ClientReadCountMismatch,
  ClientReadDigestMismatch,
  ClientDuplicateCorrelation,
  LogicalTransactionDuplicate,
  AttemptDuplicate,
  MultipleCommittedAttempts,
  SessionUnknown,
  SessionSequenceNonIncreasing,
  SessionAttemptProgressInvalid,
  RunIdMismatch,
  MarkerDuplicate,
  CorrelationMismatch,
  CdcOrderInvalid,
  FenceOrderInvalid,
  CommitSequenceInvalid,
  CheckerInputRejected,
  BootstrapStateMismatch,
  PhaseAnchorFailed,
  JoinBufferLimitExceeded,
  CheckerStateLimitExceeded,
  ResourceExhausted,
  InternalError,
};

[[nodiscard]] std::string_view toString(MonitorErrorCode code) noexcept;

class MonitorError final : public std::runtime_error {
public:
  MonitorError(MonitorErrorCode code, std::string message);

  [[nodiscard]] MonitorErrorCode code() const noexcept { return code_; }

private:
  MonitorErrorCode code_;
};

} // namespace blackbox
