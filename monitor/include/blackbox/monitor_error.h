#pragma once

#include <stdexcept>
#include <string>
#include <string_view>

namespace blackbox {

enum class MonitorErrorCode {
  ConfigInvalid,
  BranchContractMismatch,
  IsolationMismatch,
  SessionContractMismatch,
  UnsupportedTransaction,
  UnsupportedReadSource,
  UnexpectedTable,
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
  SessionDoneMissing,
  SessionDoneMismatch,
  ClientRecordAfterDone,
  RunIdMismatch,
  RunManifestMissing,
  RunManifestDuplicate,
  MarkerDuplicate,
  CorrelationMismatch,
  CdcOrderInvalid,
  FenceOrderInvalid,
  CommitSequenceInvalid,
  CheckerInputRejected,
  BootstrapStateMismatch,
  BootstrapManifestMissing,
  PhaseAnchorFailed,
  FenceMissing,
  ClientHalfMissing,
  CdcHalfMissing,
  ReconciliationMismatch,
  RedisPublicationFailed,
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
