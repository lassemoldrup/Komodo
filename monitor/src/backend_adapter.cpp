#include "blackbox/backend_adapter.h"

#include "blackbox/monitor_error.h"

#include <algorithm>
#include <array>
#include <string>

namespace blackbox {
namespace {

constexpr std::string_view pinned_sql_mode =
    "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,"
    "NO_ENGINE_SUBSTITUTION,PAD_CHAR_TO_FULL_LENGTH";

class MariaDbBackendAdapter final : public BackendAdapter {
public:
  [[nodiscard]] std::string_view name() const noexcept override {
    return "mariadb";
  }

  [[nodiscard]] BackendServerDefaults serverDefaults() const noexcept override {
    return BackendServerDefaults{
        .character_set = "utf8mb4",
        .collation = "utf8mb4_bin",
        .time_zone = "+00:00",
        .sql_mode = pinned_sql_mode,
        .transaction_isolation = "REPEATABLE_READ",
        .snapshot_isolation = true,
    };
  }

  [[nodiscard]] BackendSessionRequirements
  sessionRequirements() const noexcept override {
    return BackendSessionRequirements{
        .set_names = "SET NAMES utf8mb4 COLLATE utf8mb4_bin",
        .character_set_client = "utf8mb4",
        .character_set_connection = "utf8mb4",
        .character_set_results = "utf8mb4",
        .collation_connection = "utf8mb4_bin",
        .time_zone = "+00:00",
        .sql_mode = pinned_sql_mode,
        .transaction_isolation = "REPEATABLE_READ",
        .snapshot_isolation = true,
    };
  }

  [[nodiscard]] BackendDatabaseDefaults
  databaseDefaults() const noexcept override {
    return BackendDatabaseDefaults{.character_set = "utf8mb4",
                                   .collation = "utf8mb4_bin"};
  }

  [[nodiscard]] ApplicationTableRequirements
  applicationTableRequirements() const noexcept override {
    return ApplicationTableRequirements{
        .engine = "InnoDB",
        .character_set = "utf8mb4",
        .collation = "utf8mb4_bin",
        .char_read_semantics = "pad_to_declared_length",
    };
  }
};

const MariaDbBackendAdapter mariadb_adapter;
const std::array<const BackendAdapter *, 1> registered_adapters = {
    &mariadb_adapter,
};

} // namespace

std::span<const BackendAdapter *const>
BackendAdapterRegistry::adapters() noexcept {
  return registered_adapters;
}

const BackendAdapter *
BackendAdapterRegistry::find(const std::string_view backend) noexcept {
  const auto found =
      std::find_if(registered_adapters.begin(), registered_adapters.end(),
                   [backend](const BackendAdapter *adapter) {
                     return adapter != nullptr && adapter->name() == backend;
                   });
  return found == registered_adapters.end() ? nullptr : *found;
}

const BackendAdapter &
BackendAdapterRegistry::require(const std::string_view backend) {
  const auto *adapter = find(backend);
  if (adapter == nullptr) {
    throw MonitorError(MonitorErrorCode::ConfigInvalid,
                       "RunManifest names an unregistered backend adapter");
  }
  return *adapter;
}

} // namespace blackbox
