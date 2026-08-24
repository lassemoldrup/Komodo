#include "blackbox/cdc_error.h"

#include <cstdlib>
#include <utility>

namespace blackbox {

std::string_view toString(const CdcErrorCode code) noexcept {
  switch (code) {
  case CdcErrorCode::CdcRecordMalformed:
    return "CDC_RECORD_MALFORMED";
  case CdcErrorCode::CdcBeginMissing:
    return "CDC_BEGIN_MISSING";
  case CdcErrorCode::CdcEndMissing:
    return "CDC_END_MISSING";
  case CdcErrorCode::CdcTransactionIdMismatch:
    return "CDC_TRANSACTION_ID_MISMATCH";
  case CdcErrorCode::CdcEventOrdinalInvalid:
    return "CDC_EVENT_ORDINAL_INVALID";
  case CdcErrorCode::CdcEventCountMismatch:
    return "CDC_EVENT_COUNT_MISMATCH";
  case CdcErrorCode::CdcDuplicateConflict:
    return "CDC_DUPLICATE_CONFLICT";
  case CdcErrorCode::MarkerMissing:
    return "MARKER_MISSING";
  case CdcErrorCode::MarkerMultiple:
    return "MARKER_MULTIPLE";
  case CdcErrorCode::MarkerMalformed:
    return "MARKER_MALFORMED";
  case CdcErrorCode::MarkerNotLast:
    return "MARKER_NOT_LAST";
  case CdcErrorCode::MarkerDuplicate:
    return "MARKER_DUPLICATE";
  case CdcErrorCode::UnmarkedApplicationTransaction:
    return "UNMARKED_APPLICATION_TRANSACTION";
  case CdcErrorCode::CdcApplicationAfterEnd:
    return "CDC_APPLICATION_AFTER_END";
  case CdcErrorCode::CdcOrderInvalid:
    return "CDC_ORDER_INVALID";
  case CdcErrorCode::UnexpectedTable:
    return "UNEXPECTED_TABLE";
  case CdcErrorCode::DdlObserved:
    return "DDL_OBSERVED";
  case CdcErrorCode::ExternalWriterObserved:
    return "EXTERNAL_WRITER_OBSERVED";
  case CdcErrorCode::CanonicalEncodingFailed:
    return "CANONICAL_ENCODING_FAILED";
  case CdcErrorCode::PrimaryKeyImageIncomplete:
    return "PRIMARY_KEY_IMAGE_INCOMPLETE";
  case CdcErrorCode::RunIdMismatch:
    return "RUN_ID_MISMATCH";
  case CdcErrorCode::FenceDuplicate:
    return "FENCE_DUPLICATE";
  case CdcErrorCode::FenceOrderInvalid:
    return "FENCE_ORDER_INVALID";
  case CdcErrorCode::ComponentRestarted:
    return "COMPONENT_RESTARTED";
  case CdcErrorCode::ResourceExhausted:
    return "RESOURCE_EXHAUSTED";
  }
  std::abort();
}

CdcAssemblerError::CdcAssemblerError(const CdcErrorCode code,
                                     std::string message, std::string stream_id)
    : std::runtime_error(std::move(message)), code_(code),
      stream_id_(std::move(stream_id)) {}

} // namespace blackbox
