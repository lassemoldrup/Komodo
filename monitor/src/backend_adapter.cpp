#include "blackbox/backend_adapter.h"

#include "blackbox/monitor_error.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

namespace blackbox {
namespace {

constexpr std::string_view pinned_sql_mode =
    "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,"
    "NO_ENGINE_SUBSTITUTION,PAD_CHAR_TO_FULL_LENGTH";

using Json = nlohmann::json;

struct QualifiedTable {
  std::string schema;
  std::string table;
  std::string full_name;
};

[[nodiscard]] bool isCanonicalIdentifier(const std::string_view value) {
  if (value.empty() || value.size() > 64U || value.front() < 'a' ||
      value.front() > 'z') {
    return false;
  }
  return std::all_of(value.begin() + 1, value.end(), [](const char character) {
    return (character >= 'a' && character <= 'z') ||
           (character >= '0' && character <= '9') || character == '_';
  });
}

[[nodiscard]] std::optional<QualifiedTable>
parseQualifiedTable(const std::string_view value) {
  const auto separator = value.find('.');
  if (separator == std::string_view::npos || separator == 0U ||
      separator + 1U >= value.size() ||
      value.find('.', separator + 1U) != std::string_view::npos) {
    return std::nullopt;
  }
  const auto schema = value.substr(0, separator);
  const auto table = value.substr(separator + 1U);
  if (!isCanonicalIdentifier(schema) || !isCanonicalIdentifier(table)) {
    return std::nullopt;
  }
  return QualifiedTable{.schema = std::string(schema),
                        .table = std::string(table),
                        .full_name = std::string(value)};
}

[[noreturn]] void schemaError(const std::string &message) {
  throw AdapterSchemaError(message);
}

void requireObject(const Json &value, const std::string_view label) {
  if (!value.is_object()) {
    schemaError(std::string(label) + " is not an object");
  }
}

void requireExactKeys(const Json &object,
                      const std::initializer_list<std::string_view> expected,
                      const std::string_view label) {
  requireObject(object, label);
  std::unordered_set<std::string_view> remaining(expected.begin(),
                                                 expected.end());
  for (const auto &[key, unused] : object.items()) {
    (void)unused;
    if (remaining.erase(key) == 0U) {
      schemaError(std::string(label) + " contains unexpected field " + key);
    }
  }
  if (!remaining.empty()) {
    schemaError(std::string(label) + " is missing a required field");
  }
}

[[nodiscard]] const Json &member(const Json &object,
                                 const std::string_view name,
                                 const std::string_view label) {
  requireObject(object, label);
  const auto found = object.find(name);
  if (found == object.end()) {
    schemaError(std::string(label) + " is missing field " + std::string(name));
  }
  return *found;
}

[[nodiscard]] std::string stringValue(const Json &value,
                                      const std::string_view label) {
  if (!value.is_string()) {
    schemaError(std::string(label) + " is not a string");
  }
  return value.get<std::string>();
}

[[nodiscard]] std::uint64_t uintValue(
    const Json &value, const std::string_view label,
    const std::uint64_t maximum = std::numeric_limits<std::uint64_t>::max()) {
  std::uint64_t result = 0;
  if (value.is_number_unsigned()) {
    result = value.get<std::uint64_t>();
  } else if (value.is_number_integer()) {
    const auto signed_value = value.get<std::int64_t>();
    if (signed_value < 0) {
      schemaError(std::string(label) + " is negative");
    }
    result = static_cast<std::uint64_t>(signed_value);
  } else {
    schemaError(std::string(label) + " is not an integer");
  }
  if (result > maximum) {
    schemaError(std::string(label) + " exceeds its supported range");
  }
  return result;
}

void validateNullableString(const Json &value, const std::string_view label) {
  if (!value.is_null() && !value.is_string()) {
    schemaError(std::string(label) + " is neither null nor a string");
  }
}

[[nodiscard]] std::optional<std::uint64_t>
nullableUint(const Json &value, const std::string_view label) {
  if (value.is_null()) {
    return std::nullopt;
  }
  return uintValue(value, label);
}

[[nodiscard]] std::optional<std::string> nullableString(const Json &value) {
  if (value.is_null()) {
    return std::nullopt;
  }
  return stringValue(value, "startup schema nullable string");
}

class MariaDbStartupSchemaDecoder final : public CdcStartupSchemaDecoder {
public:
  explicit MariaDbStartupSchemaDecoder(CdcStartupSchemaConfig config)
      : source_id_(std::move(config.backend_source_id)),
        debezium_version_(std::move(config.debezium_version)) {
    std::unordered_set<std::string> configured;
    const auto add_table = [&](const std::string &name,
                               const bool application) {
      const auto parsed = parseQualifiedTable(name);
      if (!parsed.has_value() || !configured.insert(name).second) {
        throw std::invalid_argument(
            "startup schema decoder table roster is invalid");
      }
      if (database_.empty()) {
        database_ = parsed->schema;
      } else if (database_ != parsed->schema) {
        throw std::invalid_argument(
            "startup schema decoder tables span multiple namespaces");
      }
      configured_tables_.push_back(*parsed);
      if (application) {
        application_tables_.insert(parsed->full_name);
      }
    };
    for (const auto &name : config.application_tables) {
      add_table(name, true);
    }
    for (const auto &name : config.control_tables) {
      add_table(name, false);
    }
    if (source_id_.empty() || debezium_version_.empty() || database_.empty()) {
      throw std::invalid_argument("startup schema decoder identity is invalid");
    }
  }

  [[nodiscard]] CdcStartupSchemaObservation
  accept(const Json &key, const Json &envelope) override {
    if (complete_) {
      schemaError("startup schema record follows the completed snapshot");
    }
    requireExactKeys(key, {"databaseName"}, "startup schema snapshot key");
    requireExactKeys(envelope,
                     {"source", "ts_ms", "databaseName", "schemaName", "ddl",
                      "tableChanges"},
                     "startup schema snapshot envelope");
    const auto key_database =
        stringValue(member(key, "databaseName", "startup schema snapshot key"),
                    "startup schema snapshot key.databaseName");
    const auto database =
        stringValue(member(envelope, "databaseName", "startup schema snapshot"),
                    "startup schema snapshot.databaseName");
    if (key_database != database ||
        (!database.empty() && database != database_)) {
      schemaError("startup schema snapshot database identity is invalid");
    }
    if (!member(envelope, "schemaName", "startup schema snapshot").is_null()) {
      schemaError(
          "startup schema snapshot schemaName is unexpectedly non-null");
    }
    const auto ddl =
        stringValue(member(envelope, "ddl", "startup schema snapshot"),
                    "startup schema snapshot.ddl");
    if (ddl.empty()) {
      schemaError("startup schema snapshot DDL is empty");
    }
    (void)uintValue(member(envelope, "ts_ms", "startup schema snapshot"),
                    "startup schema snapshot.ts_ms");
    const auto &changes =
        member(envelope, "tableChanges", "startup schema snapshot");

    const auto &source = member(envelope, "source", "startup schema snapshot");
    requireExactKeys(source,
                     {"version", "connector", "name", "ts_ms", "snapshot", "db",
                      "sequence", "ts_us", "ts_ns", "table", "server_id",
                      "gtid", "file", "pos", "row", "thread", "query"},
                     "startup schema snapshot source");
    if (stringValue(member(source, "version", "startup schema source"),
                    "startup schema source.version") != debezium_version_ ||
        stringValue(member(source, "connector", "startup schema source"),
                    "startup schema source.connector") != "mariadb" ||
        stringValue(member(source, "name", "startup schema source"),
                    "startup schema source.name") != source_id_ ||
        stringValue(member(source, "db", "startup schema source"),
                    "startup schema source.db") != database) {
      schemaError("startup schema snapshot source identity is invalid");
    }
    const auto snapshot =
        stringValue(member(source, "snapshot", "startup schema source"),
                    "startup schema source.snapshot");
    if (snapshot != "true" && snapshot != "last") {
      schemaError("non-snapshot DDL was observed");
    }
    for (const auto field : {"sequence", "gtid", "thread", "query"}) {
      if (!member(source, field, "startup schema source").is_null()) {
        schemaError("startup schema source nullable metadata is non-null");
      }
    }
    const auto &table_value = member(source, "table", "startup schema source");
    std::optional<std::string> source_table;
    if (!table_value.is_null()) {
      const auto table_name =
          stringValue(table_value, "startup schema source.table");
      if (database.empty() || !isCanonicalIdentifier(table_name)) {
        schemaError("startup schema source table identity is invalid");
      }
      source_table = table_name;
    }
    if (uintValue(member(source, "server_id", "startup schema source"),
                  "startup schema source.server_id") != 0U) {
      schemaError("startup schema snapshot server_id is not zero");
    }
    if (stringValue(member(source, "file", "startup schema source"),
                    "startup schema source.file")
            .empty()) {
      schemaError("startup schema snapshot source.file is empty");
    }
    for (const auto field : {"ts_ms", "ts_us", "ts_ns", "pos", "row"}) {
      (void)uintValue(member(source, field, "startup schema source"),
                      std::string("startup schema source.") + field);
    }

    auto observation =
        acceptSemantics(database, source_table, ddl, changes, snapshot);
    saw_record_ = true;
    observation.complete = complete_;
    return observation;
  }

  [[nodiscard]] bool sawRecord() const noexcept override { return saw_record_; }

  [[nodiscard]] bool complete() const noexcept override { return complete_; }

private:
  enum class Stage {
    ServerSettings,
    DropTables,
    CreateDatabase,
    UseDatabase,
    CreateTables,
    Complete,
  };

  [[nodiscard]] static std::string
  snapshotTableIdentifier(const QualifiedTable &table) {
    return "\"" + table.schema + "\".\"" + table.table + "\"";
  }

  void requireConfiguredSnapshotTables() const {
    for (const auto &table : configured_tables_) {
      if (!std::binary_search(dropped_table_ids_.begin(),
                              dropped_table_ids_.end(),
                              snapshotTableIdentifier(table))) {
        schemaError("startup schema snapshot is missing a configured table");
      }
    }
  }

  static void requireSnapshotFlag(const std::string_view actual,
                                  const std::string_view expected) {
    if (actual != expected) {
      schemaError("startup schema snapshot marker is out of order");
    }
  }

  void validateSnapshotSchemaColumn(const Json &column) const {
    requireExactKeys(column,
                     {"name", "jdbcType", "nativeType", "typeName",
                      "typeExpression", "charsetName", "length", "scale",
                      "position", "optional", "autoIncremented", "generated",
                      "comment", "defaultValueExpression", "enumValues"},
                     "startup schema snapshot column");
    if (stringValue(member(column, "name", "startup schema column"),
                    "startup schema column.name")
            .empty() ||
        !member(column, "jdbcType", "startup schema column")
             .is_number_integer()) {
      schemaError("startup schema snapshot column identity is malformed");
    }
    validateNullableString(
        member(column, "nativeType", "startup schema column"),
        "startup schema column.nativeType");
    (void)stringValue(member(column, "typeName", "startup schema column"),
                      "startup schema column.typeName");
    (void)stringValue(member(column, "typeExpression", "startup schema column"),
                      "startup schema column.typeExpression");
    validateNullableString(
        member(column, "charsetName", "startup schema column"),
        "startup schema column.charsetName");
    for (const auto field : {"length", "scale"}) {
      const auto &value = member(column, field, "startup schema column");
      if (!value.is_null() && !value.is_number_integer()) {
        schemaError("startup schema column numeric metadata is malformed");
      }
    }
    if (uintValue(member(column, "position", "startup schema column"),
                  "startup schema column.position",
                  std::numeric_limits<std::uint32_t>::max()) == 0U) {
      schemaError("startup schema column.position is invalid");
    }
    for (const auto field : {"optional", "autoIncremented", "generated"}) {
      if (!member(column, field, "startup schema column").is_boolean()) {
        schemaError("startup schema column boolean metadata is malformed");
      }
    }
    validateNullableString(member(column, "comment", "startup schema column"),
                           "startup schema column.comment");
    validateNullableString(
        member(column, "defaultValueExpression", "startup schema column"),
        "startup schema column.defaultValueExpression");
    const auto &enum_values =
        member(column, "enumValues", "startup schema column");
    if (!enum_values.is_null()) {
      if (!enum_values.is_array()) {
        schemaError("startup schema column.enumValues is malformed");
      }
      for (const auto &value : enum_values) {
        (void)stringValue(value, "startup schema enum value");
      }
    }
  }

  [[nodiscard]] StartupSchemaColumn
  decodeStartupSchemaColumn(const Json &column) const {
    return StartupSchemaColumn{
        .name = stringValue(member(column, "name", "startup schema column"),
                            "startup schema column.name"),
        .type_name =
            stringValue(member(column, "typeName", "startup schema column"),
                        "startup schema column.typeName"),
        .type_expression = stringValue(
            member(column, "typeExpression", "startup schema column"),
            "startup schema column.typeExpression"),
        .length =
            nullableUint(member(column, "length", "startup schema column"),
                         "startup schema column.length"),
        .scale = nullableUint(member(column, "scale", "startup schema column"),
                              "startup schema column.scale"),
        .charset_name = nullableString(
            member(column, "charsetName", "startup schema column")),
        .position =
            uintValue(member(column, "position", "startup schema column"),
                      "startup schema column.position",
                      std::numeric_limits<std::uint32_t>::max()),
        .nullable =
            member(column, "optional", "startup schema column").get<bool>(),
        .auto_incremented =
            member(column, "autoIncremented", "startup schema column")
                .get<bool>(),
        .generated =
            member(column, "generated", "startup schema column").get<bool>(),
    };
  }

  void validateSnapshotTableDefinition(const Json &table) const {
    requireExactKeys(
        table,
        {"defaultCharsetName", "primaryKeyColumnNames", "columns", "comment"},
        "startup schema snapshot table");
    validateNullableString(
        member(table, "defaultCharsetName", "startup schema table"),
        "startup schema table.defaultCharsetName");
    const auto &primary_keys =
        member(table, "primaryKeyColumnNames", "startup schema table");
    const auto &columns = member(table, "columns", "startup schema table");
    if (!primary_keys.is_array() || !columns.is_array() || columns.empty()) {
      schemaError("startup schema snapshot table arrays are malformed");
    }
    for (const auto &primary_key : primary_keys) {
      (void)stringValue(primary_key, "startup schema primary-key name");
    }
    for (const auto &column : columns) {
      validateSnapshotSchemaColumn(column);
    }
    validateNullableString(member(table, "comment", "startup schema table"),
                           "startup schema table.comment");
  }

  [[nodiscard]] StartupTableSchema
  decodeStartupTableSchema(const QualifiedTable &qualified_table,
                           const std::string &ddl, const Json &table) const {
    StartupTableSchema result{
        .name = qualified_table.full_name,
        .ddl = ddl,
        .default_charset_name = nullableString(
            member(table, "defaultCharsetName", "startup schema table")),
        .primary_key_columns = {},
        .columns = {},
    };
    const auto &primary_keys =
        member(table, "primaryKeyColumnNames", "startup schema table");
    result.primary_key_columns.reserve(primary_keys.size());
    for (const auto &primary_key : primary_keys) {
      result.primary_key_columns.push_back(
          stringValue(primary_key, "startup schema primary-key name"));
    }
    const auto &columns = member(table, "columns", "startup schema table");
    result.columns.reserve(columns.size());
    for (const auto &column : columns) {
      result.columns.push_back(decodeStartupSchemaColumn(column));
    }
    return result;
  }

  [[nodiscard]] CdcStartupSchemaObservation
  acceptSemantics(const std::string &database,
                  const std::optional<std::string> &source_table,
                  const std::string &ddl, const Json &changes,
                  const std::string &snapshot) {
    if (!changes.is_array()) {
      schemaError("startup schema snapshot tableChanges is not an array");
    }
    if (changes.empty()) {
      if (source_table.has_value()) {
        schemaError(
            "startup database schema record unexpectedly names a table");
      }
      switch (stage_) {
      case Stage::ServerSettings:
        if (!database.empty() || !std::string_view(ddl).starts_with("SET ")) {
          schemaError("startup server-settings schema record is malformed");
        }
        requireSnapshotFlag(snapshot, "true");
        stage_ = Stage::DropTables;
        return {};
      case Stage::DropTables:
        if (database != database_ ||
            !std::string_view(ddl).starts_with("DROP DATABASE ") ||
            dropped_table_ids_.empty()) {
          schemaError("startup DROP DATABASE record is out of order");
        }
        requireSnapshotFlag(snapshot, "true");
        requireConfiguredSnapshotTables();
        stage_ = Stage::CreateDatabase;
        return {};
      case Stage::CreateDatabase:
        if (database != database_ ||
            !std::string_view(ddl).starts_with("CREATE DATABASE ")) {
          schemaError("startup CREATE DATABASE record is out of order");
        }
        requireSnapshotFlag(snapshot, "true");
        stage_ = Stage::UseDatabase;
        return {};
      case Stage::UseDatabase:
        if (database != database_ ||
            !std::string_view(ddl).starts_with("USE ")) {
          schemaError("startup USE DATABASE record is out of order");
        }
        requireSnapshotFlag(snapshot, "true");
        stage_ = Stage::CreateTables;
        return {};
      case Stage::CreateTables:
      case Stage::Complete:
        schemaError("empty startup schema record is out of order");
      }
    }

    if (changes.size() != 1U || database != database_ ||
        !source_table.has_value()) {
      schemaError("startup table schema record is malformed");
    }
    const auto &change = changes.front();
    requireExactKeys(change, {"type", "id", "table"},
                     "startup schema snapshot table change");
    const auto type =
        stringValue(member(change, "type", "startup schema table change"),
                    "startup schema table change.type");
    const auto identifier =
        stringValue(member(change, "id", "startup schema table change"),
                    "startup schema table change.id");
    const QualifiedTable source_qualified{
        .schema = database,
        .table = *source_table,
        .full_name = database + "." + *source_table,
    };
    if (identifier != snapshotTableIdentifier(source_qualified)) {
      schemaError("startup schema table-change/source identity mismatch");
    }
    const auto &table = member(change, "table", "startup schema table change");

    if (stage_ == Stage::DropTables) {
      if (type != "DROP" || !table.is_null() ||
          !std::string_view(ddl).starts_with("DROP TABLE ")) {
        schemaError("startup DROP TABLE record is malformed");
      }
      requireSnapshotFlag(snapshot, "true");
      if (!dropped_table_ids_.empty() &&
          identifier <= dropped_table_ids_.back()) {
        schemaError(
            "startup DROP TABLE identifiers are duplicated or reordered");
      }
      dropped_table_ids_.push_back(identifier);
      return {};
    }

    if (stage_ != Stage::CreateTables || type != "CREATE" ||
        !std::string_view(ddl).starts_with("CREATE TABLE ")) {
      schemaError("startup CREATE TABLE record is out of order");
    }
    validateSnapshotTableDefinition(table);
    CdcStartupSchemaObservation observation;
    if (application_tables_.contains(source_qualified.full_name)) {
      observation.application_table_schema =
          decodeStartupTableSchema(source_qualified, ddl, table);
    }
    if (create_index_ >= dropped_table_ids_.size() ||
        identifier != dropped_table_ids_[create_index_]) {
      schemaError(
          "startup CREATE TABLE identifiers do not match ordered drops");
    }
    const bool final_table = create_index_ + 1U == dropped_table_ids_.size();
    requireSnapshotFlag(snapshot, final_table ? "last" : "true");
    ++create_index_;
    if (final_table) {
      stage_ = Stage::Complete;
      complete_ = true;
    }
    return observation;
  }

  std::string source_id_;
  std::string debezium_version_;
  std::string database_;
  std::vector<QualifiedTable> configured_tables_;
  std::unordered_set<std::string> application_tables_;
  Stage stage_ = Stage::ServerSettings;
  std::vector<std::string> dropped_table_ids_;
  std::size_t create_index_ = 0;
  bool saw_record_ = false;
  bool complete_ = false;
};

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

  [[nodiscard]] std::unique_ptr<CdcStartupSchemaDecoder>
  createCdcStartupSchemaDecoder(CdcStartupSchemaConfig config) const override {
    return std::make_unique<MariaDbStartupSchemaDecoder>(std::move(config));
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
