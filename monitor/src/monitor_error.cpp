#include "blackbox/monitor_error.h"

#include <cstdlib>
#include <utility>

namespace blackbox {

std::string_view toString(const MonitorErrorCode code) noexcept {
  switch (code) {
  case MonitorErrorCode::ConfigInvalid:
    return "CONFIG_INVALID";
  case MonitorErrorCode::BranchContractMismatch:
    return "BRANCH_CONTRACT_MISMATCH";
  case MonitorErrorCode::IsolationMismatch:
    return "ISOLATION_MISMATCH";
  case MonitorErrorCode::SessionContractMismatch:
    return "SESSION_CONTRACT_MISMATCH";
  case MonitorErrorCode::SchemaAttestationMissing:
    return "SCHEMA_ATTESTATION_MISSING";
  case MonitorErrorCode::SchemaAttestationDuplicate:
    return "SCHEMA_ATTESTATION_DUPLICATE";
  case MonitorErrorCode::SchemaAttestationMalformed:
    return "SCHEMA_ATTESTATION_MALFORMED";
  case MonitorErrorCode::SchemaAttestationMismatch:
    return "SCHEMA_ATTESTATION_MISMATCH";
  case MonitorErrorCode::SchemaAttestationDrift:
    return "SCHEMA_ATTESTATION_DRIFT";
  case MonitorErrorCode::BackendFactMismatch:
    return "BACKEND_FACT_MISMATCH";
  case MonitorErrorCode::UnsupportedTransaction:
    return "UNSUPPORTED_TRANSACTION";
  case MonitorErrorCode::UnsupportedReadSource:
    return "UNSUPPORTED_READ_SOURCE";
  case MonitorErrorCode::UnexpectedTable:
    return "UNEXPECTED_TABLE";
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
  case MonitorErrorCode::SessionDoneMissing:
    return "SESSION_DONE_MISSING";
  case MonitorErrorCode::SessionDoneMismatch:
    return "SESSION_DONE_MISMATCH";
  case MonitorErrorCode::ClientRecordAfterDone:
    return "CLIENT_RECORD_AFTER_DONE";
  case MonitorErrorCode::RunIdMismatch:
    return "RUN_ID_MISMATCH";
  case MonitorErrorCode::RunManifestMissing:
    return "RUN_MANIFEST_MISSING";
  case MonitorErrorCode::RunManifestDuplicate:
    return "RUN_MANIFEST_DUPLICATE";
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
  case MonitorErrorCode::BootstrapManifestMissing:
    return "BOOTSTRAP_MANIFEST_MISSING";
  case MonitorErrorCode::PhaseAnchorFailed:
    return "PHASE_ANCHOR_FAILED";
  case MonitorErrorCode::FenceMissing:
    return "FENCE_MISSING";
  case MonitorErrorCode::ClientHalfMissing:
    return "CLIENT_HALF_MISSING";
  case MonitorErrorCode::CdcHalfMissing:
    return "CDC_HALF_MISSING";
  case MonitorErrorCode::ReconciliationMismatch:
    return "RECONCILIATION_MISMATCH";
  case MonitorErrorCode::RedisPublicationFailed:
    return "REDIS_PUBLICATION_FAILED";
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
