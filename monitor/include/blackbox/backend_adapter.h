#pragma once

#include "blackbox/benchmark_adapter.h"
#include "blackbox/protocol_records.h"

#include <nlohmann/json_fwd.hpp>

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace blackbox {

struct BackendServerDefaults {
  std::string_view character_set;
  std::string_view collation;
  std::string_view time_zone;
  std::string_view sql_mode;
  std::string_view transaction_isolation;
  bool snapshot_isolation = false;

  bool operator==(const BackendServerDefaults &) const = default;
};

struct BackendSessionRequirements {
  std::string_view set_names;
  std::string_view character_set_client;
  std::string_view character_set_connection;
  std::string_view character_set_results;
  std::string_view collation_connection;
  std::string_view time_zone;
  std::string_view sql_mode;
  std::string_view transaction_isolation;
  bool snapshot_isolation = false;

  bool operator==(const BackendSessionRequirements &) const = default;
};

struct BackendDatabaseDefaults {
  std::string_view character_set;
  std::string_view collation;

  bool operator==(const BackendDatabaseDefaults &) const = default;
};

struct CdcStartupSchemaConfig {
  std::string backend_source_id;
  std::string debezium_version;
  std::vector<std::string> application_tables;
  std::vector<std::string> control_tables;
};

struct CdcStartupSchemaObservation {
  std::optional<StartupTableSchema> application_table_schema;
  bool complete = false;
};

enum class BackendFactValueRule {
  Exact,
  Sha256,
  CanonicalIdentifier,
  PositiveInteger,
};

struct BackendFactRule {
  std::string_view name;
  BackendFactValueRule value_rule = BackendFactValueRule::Exact;
  std::string_view expected_value;

  bool operator==(const BackendFactRule &) const = default;
};

struct BackendNormalizedCapabilities {
  bool connector_startup_schema_evidence = false;
  bool full_before_after_images = true;
  bool marker_last = true;
  std::string_view transaction_framing = "contiguous_begin_rows_end";
  std::string_view commit_order = "single_stream_completed_transaction_order";

  bool operator==(const BackendNormalizedCapabilities &) const = default;
};

// Startup schema evidence is backend-owned because connector DDL envelopes are
// not part of the opaque transaction/row contract consumed by the assembler.
class CdcStartupSchemaDecoder {
public:
  virtual ~CdcStartupSchemaDecoder() = default;

  [[nodiscard]] virtual CdcStartupSchemaObservation
  accept(const nlohmann::json &key, const nlohmann::json &envelope) = 0;
  [[nodiscard]] virtual bool sawRecord() const noexcept = 0;
  [[nodiscard]] virtual bool complete() const noexcept = 0;
};

class BackendAdapter {
public:
  virtual ~BackendAdapter() = default;

  [[nodiscard]] virtual std::string_view name() const noexcept = 0;
  [[nodiscard]] virtual std::string_view connectorVersion() const noexcept = 0;
  [[nodiscard]] virtual BackendServerDefaults
  serverDefaults() const noexcept = 0;
  [[nodiscard]] virtual BackendSessionRequirements
  sessionRequirements() const noexcept = 0;
  [[nodiscard]] virtual BackendDatabaseDefaults
  databaseDefaults() const noexcept = 0;
  [[nodiscard]] virtual ApplicationTableRequirements
  applicationTableRequirements() const noexcept = 0;
  [[nodiscard]] virtual BackendNormalizedCapabilities
  normalizedCapabilities() const noexcept = 0;
  [[nodiscard]] virtual std::span<const BackendFactRule>
  attestationFactContract() const noexcept = 0;
  virtual void validateSchemaAttestation(
      const SchemaAttestation &attestation,
      const BenchmarkAdapter &benchmark_adapter) const;
  [[nodiscard]] virtual std::unique_ptr<CdcStartupSchemaDecoder>
  createCdcStartupSchemaDecoder(CdcStartupSchemaConfig config) const = 0;
};

class BackendAdapterRegistry final {
public:
  BackendAdapterRegistry() = delete;

  [[nodiscard]] static std::span<const BackendAdapter *const>
  adapters() noexcept;
  [[nodiscard]] static const BackendAdapter *
  find(std::string_view backend) noexcept;
  [[nodiscard]] static const BackendAdapter &require(std::string_view backend);
};

// Injectable non-owning catalog used by registry conformance tests and future
// dynamically loaded backend modules. Production uses BackendAdapterRegistry.
class BackendAdapterCatalog final {
public:
  explicit BackendAdapterCatalog(
      std::span<const BackendAdapter *const> adapters);

  [[nodiscard]] const BackendAdapter *
  find(std::string_view backend) const noexcept;
  [[nodiscard]] const BackendAdapter &require(std::string_view backend) const;

private:
  std::span<const BackendAdapter *const> adapters_;
};

} // namespace blackbox
