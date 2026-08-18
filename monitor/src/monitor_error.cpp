#include "blackbox/monitor_error.h"

#include <cstdlib>
#include <utility>

namespace blackbox {

std::string_view toString(const MonitorErrorCode code) noexcept {
  switch (code) {
  case MonitorErrorCode::UnsupportedTransaction:
    return "UNSUPPORTED_TRANSACTION";
  case MonitorErrorCode::UnsupportedReadSource:
    return "UNSUPPORTED_READ_SOURCE";
  case MonitorErrorCode::CanonicalEncodingFailed:
    return "CANONICAL_ENCODING_FAILED";
  case MonitorErrorCode::ClientRecordMalformed:
    return "CLIENT_RECORD_MALFORMED";
  case MonitorErrorCode::ClientReadCountMismatch:
    return "CLIENT_READ_COUNT_MISMATCH";
  case MonitorErrorCode::ClientReadDigestMismatch:
    return "CLIENT_READ_DIGEST_MISMATCH";
  case MonitorErrorCode::ClientDuplicateCorrelation:
    return "CLIENT_DUPLICATE_CORRELATION";
  case MonitorErrorCode::LogicalTransactionDuplicate:
    return "LOGICAL_TRANSACTION_DUPLICATE";
  case MonitorErrorCode::AttemptDuplicate:
    return "ATTEMPT_DUPLICATE";
  case MonitorErrorCode::MultipleCommittedAttempts:
    return "MULTIPLE_COMMITTED_ATTEMPTS";
  case MonitorErrorCode::SessionUnknown:
    return "SESSION_UNKNOWN";
  case MonitorErrorCode::SessionSequenceNonIncreasing:
    return "SESSION_SEQUENCE_NON_INCREASING";
  case MonitorErrorCode::SessionAttemptProgressInvalid:
    return "SESSION_ATTEMPT_PROGRESS_INVALID";
  case MonitorErrorCode::RunIdMismatch:
    return "RUN_ID_MISMATCH";
  case MonitorErrorCode::MarkerDuplicate:
    return "MARKER_DUPLICATE";
  case MonitorErrorCode::CorrelationMismatch:
    return "CORRELATION_MISMATCH";
  case MonitorErrorCode::CdcOrderInvalid:
    return "CDC_ORDER_INVALID";
  case MonitorErrorCode::FenceOrderInvalid:
    return "FENCE_ORDER_INVALID";
  case MonitorErrorCode::CommitSequenceInvalid:
    return "COMMIT_SEQUENCE_INVALID";
  case MonitorErrorCode::CheckerInputRejected:
    return "CHECKER_INPUT_REJECTED";
  case MonitorErrorCode::BootstrapStateMismatch:
    return "BOOTSTRAP_STATE_MISMATCH";
  case MonitorErrorCode::PhaseAnchorFailed:
    return "PHASE_ANCHOR_FAILED";
  case MonitorErrorCode::JoinBufferLimitExceeded:
    return "JOIN_BUFFER_LIMIT_EXCEEDED";
  case MonitorErrorCode::CheckerStateLimitExceeded:
    return "CHECKER_STATE_LIMIT_EXCEEDED";
  case MonitorErrorCode::ResourceExhausted:
    return "RESOURCE_EXHAUSTED";
  case MonitorErrorCode::InternalError:
    return "INTERNAL_ERROR";
  }
  std::abort();
}

MonitorError::MonitorError(const MonitorErrorCode code, std::string message)
    : std::runtime_error(std::move(message)), code_(code) {}

} // namespace blackbox
