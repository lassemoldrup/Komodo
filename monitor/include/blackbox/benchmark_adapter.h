#pragma once

#include "blackbox/canonical_row_codec.h"
#include "blackbox/monitor_error.h"

#include <cstdint>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace blackbox {

struct AdapterColumnContract {
  std::string_view name;
  std::string_view sql_type;
  bool nullable = false;
  bool key = false;
  std::string_view canonical_type;
  std::optional<std::string_view> generated;

  constexpr AdapterColumnContract(const std::string_view column_name,
                                  const std::string_view column_sql_type,
                                  const bool column_nullable,
                                  const bool key_column,
                                  const bool auto_incremented = false)
      : name(column_name), sql_type(column_sql_type), nullable(column_nullable),
        key(key_column),
        canonical_type(
            column_sql_type == "INT"                  ? "i32"
            : column_sql_type == "BIGINT"             ? "i64"
            : column_sql_type == "FLOAT"              ? "float32"
            : column_sql_type == "TIMESTAMP"          ? "timestamp_utc"
            : column_sql_type.starts_with("DECIMAL(") ? "fixed_decimal"
            : column_sql_type.starts_with("VARCHAR(") ? "utf8_varchar"
            : column_sql_type.starts_with("CHAR(")    ? "utf8_char"
                                                      : ""),
        generated(auto_incremented
                      ? std::optional<std::string_view>{"auto_increment"}
                      : std::nullopt) {}

  bool operator==(const AdapterColumnContract &) const = default;
};

struct AdapterTableContract {
  std::string_view name;
  std::span<const std::string_view> primary_key;
  std::span<const std::string_view> value_columns;
  std::span<const AdapterColumnContract> columns;

  bool operator==(const AdapterTableContract &) const = default;
};

enum class ResultSetClassification {
  Snapshot,
  Local,
  Omit,
};

enum class ResultSetShape {
  ExactKeyRow,
  ReturnedRows,
  AggregateScalar,
};

struct AdapterResultSetContract {
  std::string_view transaction_type;
  std::string_view procedure;
  std::string_view statement_id;
  std::optional<std::string_view> table;
  ResultSetClassification classification = ResultSetClassification::Omit;
  ResultSetShape result_shape = ResultSetShape::AggregateScalar;
  std::string_view empty_result;
  std::span<const std::string_view> semantics_ignored;
  std::optional<std::string_view> omission_reason;

  bool operator==(const AdapterResultSetContract &) const = default;
};

struct AdapterRuntimeCapabilities {
  bool client_trace = false;
  bool cdc_normalization = false;
  bool bootstrap_scan = false;

  [[nodiscard]] bool complete() const noexcept {
    return client_trace && cdc_normalization && bootstrap_scan;
  }

  [[nodiscard]] bool
  satisfies(const AdapterRuntimeCapabilities &required) const noexcept {
    return (!required.client_trace || client_trace) &&
           (!required.cdc_normalization || cdc_normalization) &&
           (!required.bootstrap_scan || bootstrap_scan);
  }

  bool operator==(const AdapterRuntimeCapabilities &) const = default;
};

struct StartupSchemaColumn {
  std::string name;
  std::string type_name;
  std::string type_expression;
  std::optional<std::uint64_t> length;
  std::optional<std::uint64_t> scale;
  std::optional<std::string> charset_name;
  std::uint64_t position = 0;
  bool nullable = false;
  bool auto_incremented = false;
  bool generated = false;

  bool operator==(const StartupSchemaColumn &) const = default;
};

struct StartupTableSchema {
  std::string name;
  std::string ddl;
  std::optional<std::string> default_charset_name;
  std::vector<std::string> primary_key_columns;
  std::vector<StartupSchemaColumn> columns;

  bool operator==(const StartupTableSchema &) const = default;
};

struct ApplicationTableRequirements {
  std::string_view engine;
  std::string_view character_set;
  std::string_view collation;
  std::string_view char_read_semantics;

  bool operator==(const ApplicationTableRequirements &) const = default;
};

class AdapterSchemaError final : public std::runtime_error {
public:
  using std::runtime_error::runtime_error;
};

// Immutable metadata and codec dispatch for one benchmark. The lifecycle sees
// this interface only; it never selects behavior with benchmark-name branches.
class BenchmarkAdapter {
public:
  virtual ~BenchmarkAdapter() = default;

  [[nodiscard]] virtual std::string_view name() const noexcept = 0;
  [[nodiscard]] virtual std::span<const std::string_view>
  transactionTypes() const noexcept = 0;
  [[nodiscard]] virtual std::span<const std::string_view>
  acceptedIsolations() const noexcept = 0;
  [[nodiscard]] virtual std::span<const AdapterTableContract>
  tables() const noexcept = 0;
  [[nodiscard]] virtual std::span<const AdapterResultSetContract>
  resultSets() const noexcept = 0;
  [[nodiscard]] virtual AdapterRuntimeCapabilities
  runtimeCapabilities() const noexcept = 0;
  [[nodiscard]] virtual const CanonicalRowCodec *
  rowCodec(std::string_view qualified_table) const noexcept = 0;

  [[nodiscard]] bool supportsTransaction(std::string_view transaction) const;
  [[nodiscard]] bool acceptsIsolation(std::string_view isolation) const;
  [[nodiscard]] const AdapterTableContract *
  table(std::string_view qualified_table) const noexcept;

  // Validates only adapter-owned manifest semantics. Generic protocol shape,
  // lifecycle fields, and control tables remain owned by their existing layers.
  void validateRunConfiguration(
      std::string_view declared_isolation,
      std::span<const std::string> application_table_allowlist,
      AdapterRuntimeCapabilities required_capabilities) const;
  void validateApplicationTableRoster(
      std::span<const std::string> application_table_allowlist) const;
  void validateStartupTableSchema(
      const StartupTableSchema &schema,
      const ApplicationTableRequirements &backend_requirements) const;

  // Validate canonical client bytes through the registered table codec.
  void
  validateClientRead(std::span<const std::byte> key,
                     std::optional<std::span<const std::byte>> value) const;
};

class BenchmarkAdapterRegistry final {
public:
  BenchmarkAdapterRegistry() = delete;

  [[nodiscard]] static std::span<const BenchmarkAdapter *const>
  adapters() noexcept;
  [[nodiscard]] static const BenchmarkAdapter *
  find(std::string_view benchmark) noexcept;
  [[nodiscard]] static const BenchmarkAdapter &
  require(std::string_view benchmark);
};

[[nodiscard]] bool isProtocolName(std::string_view value) noexcept;
[[nodiscard]] std::string_view
toString(ResultSetClassification classification) noexcept;
[[nodiscard]] std::string_view toString(ResultSetShape shape) noexcept;

} // namespace blackbox
