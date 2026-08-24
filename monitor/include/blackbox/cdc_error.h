#pragma once

#include <stdexcept>
#include <string>
#include <string_view>

namespace blackbox {

enum class CdcErrorCode {
  CdcRecordMalformed,
  CdcBeginMissing,
  CdcEndMissing,
  CdcTransactionIdMismatch,
  CdcEventOrdinalInvalid,
  CdcEventCountMismatch,
  CdcDuplicateConflict,
  MarkerMissing,
  MarkerMultiple,
  MarkerMalformed,
  MarkerNotLast,
  MarkerDuplicate,
  UnmarkedApplicationTransaction,
  CdcApplicationAfterEnd,
  CdcOrderInvalid,
  UnexpectedTable,
  DdlObserved,
  ExternalWriterObserved,
  CanonicalEncodingFailed,
  PrimaryKeyImageIncomplete,
  RunIdMismatch,
  FenceDuplicate,
  FenceOrderInvalid,
  ComponentRestarted,
  ResourceExhausted,
};

[[nodiscard]] std::string_view toString(CdcErrorCode code) noexcept;

class CdcAssemblerError final : public std::runtime_error {
public:
  CdcAssemblerError(CdcErrorCode code, std::string message,
                    std::string stream_id = {});

  [[nodiscard]] CdcErrorCode code() const noexcept { return code_; }
  [[nodiscard]] const std::string &streamId() const noexcept {
    return stream_id_;
  }

private:
  CdcErrorCode code_;
  std::string stream_id_;
};

} // namespace blackbox
