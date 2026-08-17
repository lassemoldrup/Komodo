#pragma once

#include "blackbox/canonical_ycsb_codec.h"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace blackbox {

enum class CdcTransactionKind {
  RunControl,
  BootstrapApp,
  WorkloadMarked,
};

enum class RunFencePhase {
  RunStart,
  LoadFinished,
  EndFence,
};

enum class NormalizedValueState {
  Present,
  Absent,
};

struct RunScopedCorrelationId {
  std::string run_id;
  std::string session_id;
  std::uint64_t txn_id = 0;

  bool operator==(const RunScopedCorrelationId &) const = default;
};

struct NormalizedWrite {
  CanonicalBytes key;
  NormalizedValueState state = NormalizedValueState::Absent;
  std::optional<CanonicalBytes> value;

  bool operator==(const NormalizedWrite &) const = default;
};

struct CdcTransaction {
  std::uint64_t cdc_seq = 0;
  CdcTransactionKind kind = CdcTransactionKind::BootstrapApp;
  std::optional<RunFencePhase> fence_phase;
  std::optional<RunScopedCorrelationId> correlation_id;
  std::vector<NormalizedWrite> writes;
  std::string begin_stream_id;
  std::string end_stream_id;

  bool operator==(const CdcTransaction &) const = default;
};

[[nodiscard]] std::string_view toString(CdcTransactionKind kind) noexcept;
[[nodiscard]] std::string_view toString(RunFencePhase phase) noexcept;
[[nodiscard]] std::string_view toString(NormalizedValueState state) noexcept;

} // namespace blackbox
