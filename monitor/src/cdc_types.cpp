#include "blackbox/cdc_types.h"

#include <cstdlib>

namespace blackbox {

std::string_view toString(const CdcTransactionKind kind) noexcept {
  switch (kind) {
  case CdcTransactionKind::RunControl:
    return "RUN_CONTROL";
  case CdcTransactionKind::BootstrapApp:
    return "BOOTSTRAP_APP";
  case CdcTransactionKind::WorkloadMarked:
    return "WORKLOAD_MARKED";
  }
  std::abort();
}

std::string_view toString(const RunFencePhase phase) noexcept {
  switch (phase) {
  case RunFencePhase::RunStart:
    return "RUN_START";
  case RunFencePhase::LoadFinished:
    return "LOAD_FINISHED";
  case RunFencePhase::EndFence:
    return "END_FENCE";
  }
  std::abort();
}

std::string_view toString(const NormalizedValueState state) noexcept {
  switch (state) {
  case NormalizedValueState::Present:
    return "present";
  case NormalizedValueState::Absent:
    return "absent";
  }
  std::abort();
}

} // namespace blackbox
