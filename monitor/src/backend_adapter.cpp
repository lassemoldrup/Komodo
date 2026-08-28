#include "blackbox/backend_adapter.h"

#include "blackbox/monitor_error.h"

#include "strict_json.h"

#include <nlohmann/json.hpp>
#include <simdjson.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <ctime>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
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

void requireKeys(const Json &object,
                 const std::initializer_list<std::string_view> required,
                 const std::initializer_list<std::string_view> optional,
                 const std::string_view label) {
  requireObject(object, label);
  std::unordered_set<std::string_view> remaining(required.begin(),
                                                 required.end());
  std::unordered_set<std::string_view> allowed(optional.begin(),
                                               optional.end());
  for (const auto &[key, unused] : object.items()) {
    (void)unused;
    if (remaining.erase(key) == 0U && !allowed.contains(key)) {
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

class CatalogOnlyStartupSchemaDecoder final : public CdcStartupSchemaDecoder {
public:
  [[nodiscard]] CdcStartupSchemaObservation accept(const Json &,
                                                   const Json &) override {
    schemaError("backend does not admit connector-produced schema records");
  }

  [[nodiscard]] bool sawRecord() const noexcept override { return true; }
  [[nodiscard]] bool complete() const noexcept override { return true; }
};

struct RawCdcParserState {
  simdjson::ondemand::parser parser;
  std::string input;
};

[[nodiscard]] RawCdcParserState &rawCdcParserState() {
  thread_local RawCdcParserState state;
  return state;
}

[[noreturn]] void rawCdcMalformed(const std::string &message) {
  throw BackendCdcDecodeError(CdcErrorCode::CdcRecordMalformed, message);
}

[[nodiscard]] std::string_view rawCdcText(simdjson::ondemand::value value,
                                          const std::string_view label,
                                          const std::size_t maximum = 512U) {
  const auto text = std::string_view(value.get_string());
  if (text.empty() || text.size() > maximum) {
    rawCdcMalformed(std::string(label) + " is empty or oversized");
  }
  return text;
}

[[nodiscard]] std::string rawCdcString(simdjson::ondemand::value value,
                                       const std::string_view label,
                                       const std::size_t maximum = 512U) {
  return std::string(rawCdcText(value, label, maximum));
}

[[nodiscard]] std::uint64_t rawCdcUint(simdjson::ondemand::value value,
                                       const std::string_view label) {
  auto token = value.raw_json_token();
  while (!token.empty() && (token.front() == ' ' || token.front() == '\t' ||
                            token.front() == '\n' || token.front() == '\r')) {
    token.remove_prefix(1U);
  }
  while (!token.empty() && (token.back() == ' ' || token.back() == '\t' ||
                            token.back() == '\n' || token.back() == '\r')) {
    token.remove_suffix(1U);
  }
  std::uint64_t result = 0;
  const auto parsed =
      std::from_chars(token.data(), token.data() + token.size(), result);
  if (token.empty() || parsed.ec != std::errc{} ||
      parsed.ptr != token.data() + token.size()) {
    rawCdcMalformed(std::string(label) + " is not a nonnegative integer");
  }
  return result;
}

[[nodiscard]] bool rawCdcIsNull(simdjson::ondemand::value value) {
  return value.type().value() == simdjson::ondemand::json_type::null;
}

void rawCdcRequireNull(simdjson::ondemand::value value,
                       const std::string_view label) {
  if (!rawCdcIsNull(value)) {
    rawCdcMalformed(std::string(label) + " is not null");
  }
}

void rawCdcRequireNullableString(simdjson::ondemand::value value,
                                 const std::string_view label) {
  const auto type = value.type().value();
  if (type == simdjson::ondemand::json_type::null) {
    return;
  }
  if (type != simdjson::ondemand::json_type::string) {
    rawCdcMalformed(std::string(label) + " is neither null nor a string");
  }
  (void)value.get_string();
}

void rawCdcRequireNullableInteger(simdjson::ondemand::value value,
                                  const std::string_view label) {
  if (!rawCdcIsNull(value)) {
    (void)rawCdcUint(value, label);
  }
}

[[nodiscard]] bool rawCdcSnapshotIsFalse(simdjson::ondemand::value value) {
  const auto type = value.type().value();
  if (type == simdjson::ondemand::json_type::boolean) {
    return !value.get_bool();
  }
  return type == simdjson::ondemand::json_type::string &&
         std::string_view(value.get_string()) == "false";
}

void rawCdcMarkField(std::uint32_t &seen, const std::uint32_t bit,
                     const std::string_view key, const std::string_view label) {
  if ((seen & bit) != 0U) {
    rawCdcMalformed(std::string(label) + " contains duplicated field " +
                    std::string(key));
  }
  seen |= bit;
}

void rawCdcRequireFields(const std::uint32_t seen, const std::uint32_t required,
                         const std::uint32_t allowed,
                         const std::string_view label) {
  if ((seen & required) != required) {
    rawCdcMalformed(std::string(label) + " is missing a required field");
  }
  if ((seen & ~allowed) != 0U) {
    rawCdcMalformed(std::string(label) + " contains an unexpected field");
  }
}

[[nodiscard]] Json rawCdcMaterialize(simdjson::ondemand::value value) {
  switch (value.type().value()) {
  case simdjson::ondemand::json_type::array: {
    Json result = Json::array();
    for (auto element : value.get_array()) {
      result.push_back(rawCdcMaterialize(element.value()));
    }
    return result;
  }
  case simdjson::ondemand::json_type::object: {
    Json result = Json::object();
    for (auto field : value.get_object()) {
      const auto key = std::string(std::string_view(field.unescaped_key()));
      if (result.contains(key)) {
        rawCdcMalformed("raw CDC row object contains duplicated field " + key);
      }
      result[key] = rawCdcMaterialize(field.value());
    }
    return result;
  }
  case simdjson::ondemand::json_type::number: {
    const auto number = value.get_number().value();
    switch (number.get_number_type()) {
    case simdjson::ondemand::number_type::signed_integer:
      return number.get_int64();
    case simdjson::ondemand::number_type::unsigned_integer:
      return number.get_uint64();
    case simdjson::ondemand::number_type::floating_point_number:
      return number.get_double();
    }
    break;
  }
  case simdjson::ondemand::json_type::string:
    return std::string(std::string_view(value.get_string()));
  case simdjson::ondemand::json_type::boolean:
    return value.get_bool().value();
  case simdjson::ondemand::json_type::null:
    return nullptr;
  }
  rawCdcMalformed("raw CDC row value has an unsupported JSON type");
}

void rawCdcConsume(simdjson::ondemand::value value) {
  switch (value.type().value()) {
  case simdjson::ondemand::json_type::array:
    for (auto element : value.get_array()) {
      rawCdcConsume(element.value());
    }
    return;
  case simdjson::ondemand::json_type::object: {
    std::unordered_set<std::string> names;
    for (auto field : value.get_object()) {
      auto name = std::string(std::string_view(field.unescaped_key()));
      if (!names.insert(name).second) {
        rawCdcMalformed("raw CDC object contains duplicated field " + name);
      }
      rawCdcConsume(field.value());
    }
    return;
  }
  case simdjson::ondemand::json_type::number:
    (void)value.get_number().value();
    return;
  case simdjson::ondemand::json_type::string:
    (void)value.get_string();
    return;
  case simdjson::ondemand::json_type::boolean:
    (void)value.get_bool();
    return;
  case simdjson::ondemand::json_type::null:
    return;
  }
  rawCdcMalformed("raw CDC value has an unsupported JSON type");
}

enum class RawMariaDbEnvelopeKind {
  StartupSchema,
  Metadata,
  Row,
};

[[nodiscard]] RawMariaDbEnvelopeKind
classifyRawMariaDbEnvelope(const std::string_view envelope_json) {
  auto &state = rawCdcParserState();
  state.input.assign(envelope_json);
  auto document = state.parser.iterate(state.input);
  bool status = false;
  bool operation = false;
  bool ddl = false;
  for (auto field : document.get_object()) {
    const auto name = std::string_view(field.unescaped_key());
    status = status || name == "status";
    operation = operation || name == "op";
    ddl = ddl || name == "ddl";
  }
  if (status != operation) {
    return status ? RawMariaDbEnvelopeKind::Metadata
                  : RawMariaDbEnvelopeKind::Row;
  }
  if (!status && ddl) {
    return RawMariaDbEnvelopeKind::StartupSchema;
  }
  rawCdcMalformed("raw CDC record is not one pinned envelope kind");
}

class PinnedEnvelopeDecoder final : public BackendCdcDecoder {
public:
  enum class Profile {
    MariaDb,
    YugabytePgoutput,
    OracleLogMiner,
  };

  PinnedEnvelopeDecoder(BackendCdcDecoderConfig config, const Profile profile)
      : source_id_(std::move(config.backend_source_id)),
        connector_version_(std::move(config.connector_version)),
        profile_(profile) {
    const auto add_table = [this](const std::string &name,
                                  const bool application) {
      const auto parsed = parseQualifiedTable(name);
      auto physical_leaf = parsed.has_value() ? parsed->table : std::string{};
      if (profile_ == Profile::OracleLogMiner) {
        std::transform(physical_leaf.begin(), physical_leaf.end(),
                       physical_leaf.begin(), [](const unsigned char value) {
                         return static_cast<char>(std::toupper(value));
                       });
      }
      if (!parsed.has_value() || !canonical_tables_.insert(name).second ||
          !canonical_by_leaf_.emplace(physical_leaf, name).second) {
        throw std::invalid_argument(
            "backend CDC decoder table roster is invalid or ambiguous");
      }
      if (profile_ == Profile::OracleLogMiner && application) {
        const AdapterTableContract *contract = nullptr;
        for (const auto *adapter : BenchmarkAdapterRegistry::adapters()) {
          if (adapter == nullptr) {
            continue;
          }
          const auto *candidate = adapter->table(name);
          if (candidate != nullptr) {
            if (contract != nullptr && contract != candidate) {
              throw std::invalid_argument(
                  "Oracle CDC table has ambiguous registered contracts");
            }
            contract = candidate;
          }
        }
        if (contract == nullptr) {
          throw std::invalid_argument(
              "Oracle CDC application table lacks a registered contract");
        }
        oracle_contracts_.emplace(name, contract);
      }
    };
    for (const auto &table : config.application_tables) {
      add_table(table, true);
    }
    for (const auto &table : config.control_tables) {
      add_table(table, false);
    }
    if (source_id_.empty() || source_id_.size() > 256U ||
        connector_version_.empty() || canonical_tables_.empty()) {
      throw std::invalid_argument("backend CDC decoder identity is invalid");
    }
  }

  [[nodiscard]] BackendCdcRecord decode(const Json &key,
                                        const Json &envelope) const override {
    requireObject(envelope, "CDC envelope");
    const bool metadata = envelope.contains("status");
    const bool row = envelope.contains("op");
    if (metadata == row) {
      if (!metadata && envelope.contains("ddl") &&
          profile_ == Profile::MariaDb) {
        BackendCdcRecord result;
        result.kind = BackendCdcRecordKind::StartupSchema;
        return result;
      }
      decodeError(CdcErrorCode::CdcRecordMalformed,
                  "raw CDC record is not one pinned envelope kind");
    }
    return metadata ? decodeMetadata(key, envelope) : decodeRow(key, envelope);
  }

  [[nodiscard]] BackendCdcRecord
  decodeRaw(const std::string_view key_json,
            const std::string_view envelope_json) const override {
    if (profile_ != Profile::MariaDb) {
      return BackendCdcDecoder::decodeRaw(key_json, envelope_json);
    }
    try {
      switch (classifyRawMariaDbEnvelope(envelope_json)) {
      case RawMariaDbEnvelopeKind::StartupSchema: {
        BackendCdcRecord result;
        result.kind = BackendCdcRecordKind::StartupSchema;
        return result;
      }
      case RawMariaDbEnvelopeKind::Metadata:
        return decodeRawMariaDbMetadata(key_json, envelope_json);
      case RawMariaDbEnvelopeKind::Row:
        return decodeRawMariaDbRow(key_json, envelope_json);
      }
    } catch (const BackendCdcDecodeError &) {
      throw;
    } catch (const std::bad_alloc &) {
      throw;
    } catch (const simdjson::simdjson_error &error) {
      decodeError(CdcErrorCode::CdcRecordMalformed,
                  std::string("invalid or ambiguous CDC JSON: ") +
                      error.what());
    }
    decodeError(CdcErrorCode::CdcRecordMalformed,
                "raw CDC record is not one pinned envelope kind");
  }

private:
  [[noreturn]] static void decodeError(const CdcErrorCode code,
                                       const std::string &message) {
    throw BackendCdcDecodeError(code, message);
  }

  static void requireObject(const Json &value, const std::string_view label) {
    if (!value.is_object()) {
      decodeError(CdcErrorCode::CdcRecordMalformed,
                  std::string(label) + " is not an object");
    }
  }

  static void
  requireExactKeys(const Json &object,
                   const std::initializer_list<std::string_view> expected,
                   const std::string_view label) {
    requireObject(object, label);
    std::unordered_set<std::string_view> remaining(expected.begin(),
                                                   expected.end());
    for (const auto &[name, unused] : object.items()) {
      (void)unused;
      if (remaining.erase(name) == 0U) {
        decodeError(CdcErrorCode::CdcRecordMalformed,
                    std::string(label) + " contains unexpected field " + name);
      }
    }
    if (!remaining.empty()) {
      decodeError(CdcErrorCode::CdcRecordMalformed,
                  std::string(label) + " is missing a required field");
    }
  }

  [[nodiscard]] static const Json &member(const Json &object,
                                          const std::string_view name,
                                          const std::string_view label) {
    requireObject(object, label);
    const auto found = object.find(name);
    if (found == object.end()) {
      decodeError(CdcErrorCode::CdcRecordMalformed, std::string(label) +
                                                        " is missing field " +
                                                        std::string(name));
    }
    return *found;
  }

  [[nodiscard]] static std::string
  stringValue(const Json &value, const std::string_view label,
              const std::size_t maximum = 512U) {
    if (!value.is_string()) {
      decodeError(CdcErrorCode::CdcRecordMalformed,
                  std::string(label) + " is not a string");
    }
    auto result = value.get<std::string>();
    if (result.empty() || result.size() > maximum) {
      decodeError(CdcErrorCode::CdcRecordMalformed,
                  std::string(label) + " is empty or oversized");
    }
    return result;
  }

  [[nodiscard]] static std::uint64_t uintValue(const Json &value,
                                               const std::string_view label) {
    if (value.is_number_unsigned()) {
      return value.get<std::uint64_t>();
    }
    if (value.is_number_integer()) {
      const auto signed_value = value.get<std::int64_t>();
      if (signed_value >= 0) {
        return static_cast<std::uint64_t>(signed_value);
      }
    }
    decodeError(CdcErrorCode::CdcRecordMalformed,
                std::string(label) + " is not a nonnegative integer");
  }

  static void requireNullableString(const Json &value,
                                    const std::string_view label) {
    if (!value.is_null() && !value.is_string()) {
      decodeError(CdcErrorCode::CdcRecordMalformed,
                  std::string(label) + " is neither null nor a string");
    }
  }

  static void requireNullableInteger(const Json &value,
                                     const std::string_view label) {
    if (!value.is_null()) {
      (void)uintValue(value, label);
    }
  }

  [[nodiscard]] static bool snapshotIsFalse(const Json &value) {
    return (value.is_boolean() && !value.get<bool>()) ||
           (value.is_string() &&
            value.get_ref<const std::string &>() == "false");
  }

  [[nodiscard]] static bool containsSetWrapper(const Json &value) {
    if (value.is_array()) {
      return std::any_of(value.begin(), value.end(), containsSetWrapper);
    }
    if (!value.is_object()) {
      return false;
    }
    if (value.contains("set")) {
      return true;
    }
    return std::any_of(value.begin(), value.end(), containsSetWrapper);
  }

  [[nodiscard]] const std::string &
  canonicalTableFromLeaf(const std::string_view leaf) const {
    const bool valid_oracle =
        profile_ == Profile::OracleLogMiner && !leaf.empty() &&
        leaf.size() <= 128U && leaf.front() >= 'A' && leaf.front() <= 'Z' &&
        std::all_of(leaf.begin() + 1U, leaf.end(), [](const char character) {
          return (character >= 'A' && character <= 'Z') ||
                 (character >= '0' && character <= '9') || character == '_';
        });
    if (!valid_oracle && !isCanonicalIdentifier(leaf)) {
      decodeError(CdcErrorCode::UnexpectedTable,
                  "raw CDC table leaf is not canonical");
    }
    const auto found = canonical_by_leaf_.find(std::string(leaf));
    if (found == canonical_by_leaf_.end()) {
      decodeError(CdcErrorCode::UnexpectedTable,
                  "raw CDC table is outside the configured roster");
    }
    return found->second;
  }

  [[nodiscard]] const std::string &
  canonicalDataCollection(const std::string_view identity) const {
    if (profile_ == Profile::OracleLogMiner) {
      constexpr std::string_view prefix = "FREEPDB1.BLACKBOX_OWNER.";
      if (!identity.starts_with(prefix)) {
        decodeError(CdcErrorCode::UnexpectedTable,
                    "Oracle data collection namespace differs");
      }
      return canonicalTableFromLeaf(identity.substr(prefix.size()));
    }
    const auto found = canonical_tables_.find(std::string(identity));
    if (found == canonical_tables_.end()) {
      decodeError(CdcErrorCode::UnexpectedTable,
                  "raw CDC data collection is outside the configured roster");
    }
    return *found;
  }

  [[nodiscard]] static std::string
  decodeRawMariaDbMetadataKey(const std::string_view key_json) {
    auto &state = rawCdcParserState();
    state.input.assign(key_json);
    auto document = state.parser.iterate(state.input);
    std::uint32_t seen = 0U;
    std::string id;
    for (auto field : document.get_object()) {
      const auto name = std::string_view(field.unescaped_key());
      if (name != "id") {
        rawCdcMalformed("transaction metadata key contains unexpected field " +
                        std::string(name));
      }
      rawCdcMarkField(seen, 1U, name, "transaction metadata key");
      id = rawCdcString(field.value(), "transaction metadata key.id");
    }
    rawCdcRequireFields(seen, 1U, 1U, "transaction metadata key");
    return id;
  }

  [[nodiscard]] BackendCdcDataCollectionCount
  decodeRawMariaDbDataCollection(simdjson::ondemand::value value) const {
    constexpr std::uint32_t collection_bit = 1U << 0U;
    constexpr std::uint32_t count_bit = 1U << 1U;
    constexpr std::uint32_t expected = collection_bit | count_bit;
    std::uint32_t seen = 0U;
    std::string identity;
    std::uint64_t count = 0U;
    for (auto field : value.get_object()) {
      const auto name = std::string_view(field.unescaped_key());
      if (name == "data_collection") {
        rawCdcMarkField(seen, collection_bit, name, "END data collection");
        identity = rawCdcString(field.value(), "END data_collection");
      } else if (name == "event_count") {
        rawCdcMarkField(seen, count_bit, name, "END data collection");
        count = rawCdcUint(field.value(), "END data_collection.event_count");
      } else {
        rawCdcMalformed("END data collection contains unexpected field " +
                        std::string(name));
      }
    }
    rawCdcRequireFields(seen, expected, expected, "END data collection");
    return BackendCdcDataCollectionCount{
        .canonical_table = canonicalDataCollection(identity),
        .event_count = count,
    };
  }

  [[nodiscard]] BackendCdcRecord
  decodeRawMariaDbMetadata(const std::string_view key_json,
                           const std::string_view envelope_json) const {
    const auto key_id = decodeRawMariaDbMetadataKey(key_json);

    constexpr std::uint32_t status_bit = 1U << 0U;
    constexpr std::uint32_t id_bit = 1U << 1U;
    constexpr std::uint32_t count_bit = 1U << 2U;
    constexpr std::uint32_t collections_bit = 1U << 3U;
    constexpr std::uint32_t timestamp_bit = 1U << 4U;
    constexpr std::uint32_t expected =
        status_bit | id_bit | count_bit | collections_bit | timestamp_bit;
    std::uint32_t seen = 0U;
    std::string status;
    std::string envelope_id;
    std::optional<std::uint64_t> event_count;
    bool event_count_is_null = false;
    bool collections_are_null = false;
    std::vector<BackendCdcDataCollectionCount> collections;

    auto &state = rawCdcParserState();
    state.input.assign(envelope_json);
    auto document = state.parser.iterate(state.input);
    for (auto field : document.get_object()) {
      const auto name = std::string_view(field.unescaped_key());
      if (name == "status") {
        rawCdcMarkField(seen, status_bit, name,
                        "transaction metadata envelope");
        status =
            rawCdcString(field.value(), "transaction metadata status", 16U);
      } else if (name == "id") {
        rawCdcMarkField(seen, id_bit, name, "transaction metadata envelope");
        envelope_id =
            rawCdcString(field.value(), "transaction metadata envelope.id");
      } else if (name == "event_count") {
        rawCdcMarkField(seen, count_bit, name, "transaction metadata envelope");
        auto value = field.value().value();
        event_count_is_null = rawCdcIsNull(value);
        if (!event_count_is_null) {
          event_count = rawCdcUint(value, "END.event_count");
        }
      } else if (name == "data_collections") {
        rawCdcMarkField(seen, collections_bit, name,
                        "transaction metadata envelope");
        auto value = field.value().value();
        collections_are_null = rawCdcIsNull(value);
        if (!collections_are_null) {
          if (value.type().value() != simdjson::ondemand::json_type::array) {
            throw BackendCdcDecodeError(CdcErrorCode::CdcEventCountMismatch,
                                        "END data_collections is not an array");
          }
          for (auto collection : value.get_array()) {
            collections.push_back(
                decodeRawMariaDbDataCollection(collection.value()));
          }
        }
      } else if (name == "ts_ms") {
        rawCdcMarkField(seen, timestamp_bit, name,
                        "transaction metadata envelope");
        (void)rawCdcUint(field.value(), "transaction metadata ts_ms");
      } else {
        rawCdcMalformed(
            "transaction metadata envelope contains unexpected field " +
            std::string(name));
      }
    }
    rawCdcRequireFields(seen, expected, expected,
                        "transaction metadata envelope");
    if (key_id != envelope_id) {
      decodeError(CdcErrorCode::CdcTransactionIdMismatch,
                  "transaction metadata key and envelope IDs disagree");
    }

    BackendCdcRecord result;
    result.diagnostic_connector_id = std::move(envelope_id);
    if (status == "BEGIN") {
      if (!event_count_is_null || !collections_are_null) {
        decodeError(CdcErrorCode::CdcRecordMalformed,
                    "BEGIN counts are not null");
      }
      result.kind = BackendCdcRecordKind::Begin;
      return result;
    }
    if (status != "END") {
      decodeError(CdcErrorCode::CdcRecordMalformed,
                  "transaction metadata status is unsupported");
    }
    if (!event_count.has_value() || collections_are_null) {
      decodeError(CdcErrorCode::CdcRecordMalformed, "END counts are null");
    }
    result.kind = BackendCdcRecordKind::End;
    result.event_count = event_count;
    result.data_collections = std::move(collections);
    return result;
  }

  [[nodiscard]] std::string
  decodeRawMariaDbSource(simdjson::ondemand::value value) const {
    constexpr std::uint32_t version_bit = 1U << 0U;
    constexpr std::uint32_t connector_bit = 1U << 1U;
    constexpr std::uint32_t name_bit = 1U << 2U;
    constexpr std::uint32_t timestamp_ms_bit = 1U << 3U;
    constexpr std::uint32_t snapshot_bit = 1U << 4U;
    constexpr std::uint32_t database_bit = 1U << 5U;
    constexpr std::uint32_t sequence_bit = 1U << 6U;
    constexpr std::uint32_t timestamp_us_bit = 1U << 7U;
    constexpr std::uint32_t timestamp_ns_bit = 1U << 8U;
    constexpr std::uint32_t table_bit = 1U << 9U;
    constexpr std::uint32_t server_id_bit = 1U << 10U;
    constexpr std::uint32_t gtid_bit = 1U << 11U;
    constexpr std::uint32_t file_bit = 1U << 12U;
    constexpr std::uint32_t position_bit = 1U << 13U;
    constexpr std::uint32_t row_bit = 1U << 14U;
    constexpr std::uint32_t thread_bit = 1U << 15U;
    constexpr std::uint32_t query_bit = 1U << 16U;
    constexpr std::uint32_t expected = (1U << 17U) - 1U;
    std::uint32_t seen = 0U;
    std::string table;
    bool snapshot_is_false = false;
    bool identity_matches = true;

    for (auto field : value.get_object()) {
      const auto name = std::string_view(field.unescaped_key());
      if (name == "version") {
        rawCdcMarkField(seen, version_bit, name, "MariaDB source metadata");
        identity_matches =
            identity_matches &&
            rawCdcText(field.value(), "source.version") == connector_version_;
      } else if (name == "connector") {
        rawCdcMarkField(seen, connector_bit, name, "MariaDB source metadata");
        identity_matches =
            identity_matches &&
            rawCdcText(field.value(), "source.connector") == "mariadb";
      } else if (name == "name") {
        rawCdcMarkField(seen, name_bit, name, "MariaDB source metadata");
        identity_matches =
            identity_matches &&
            rawCdcText(field.value(), "source.name", 256U) == source_id_;
      } else if (name == "ts_ms") {
        rawCdcMarkField(seen, timestamp_ms_bit, name,
                        "MariaDB source metadata");
        (void)rawCdcUint(field.value(), "source.ts_ms");
      } else if (name == "snapshot") {
        rawCdcMarkField(seen, snapshot_bit, name, "MariaDB source metadata");
        snapshot_is_false = rawCdcSnapshotIsFalse(field.value());
      } else if (name == "db") {
        rawCdcMarkField(seen, database_bit, name, "MariaDB source metadata");
        identity_matches =
            identity_matches &&
            rawCdcText(field.value(), "source.db", 64U) == "benchbase";
      } else if (name == "sequence") {
        rawCdcMarkField(seen, sequence_bit, name, "MariaDB source metadata");
        rawCdcRequireNull(field.value(), "MariaDB source.sequence");
      } else if (name == "ts_us") {
        rawCdcMarkField(seen, timestamp_us_bit, name,
                        "MariaDB source metadata");
        (void)rawCdcUint(field.value(), "source.ts_us");
      } else if (name == "ts_ns") {
        rawCdcMarkField(seen, timestamp_ns_bit, name,
                        "MariaDB source metadata");
        (void)rawCdcUint(field.value(), "source.ts_ns");
      } else if (name == "table") {
        rawCdcMarkField(seen, table_bit, name, "MariaDB source metadata");
        table = rawCdcString(field.value(), "source.table", 64U);
      } else if (name == "server_id") {
        rawCdcMarkField(seen, server_id_bit, name, "MariaDB source metadata");
        (void)rawCdcUint(field.value(), "source.server_id");
      } else if (name == "gtid") {
        rawCdcMarkField(seen, gtid_bit, name, "MariaDB source metadata");
        rawCdcRequireNullableString(field.value(), "source.gtid");
      } else if (name == "file") {
        rawCdcMarkField(seen, file_bit, name, "MariaDB source metadata");
        (void)rawCdcText(field.value(), "source.file");
      } else if (name == "pos") {
        rawCdcMarkField(seen, position_bit, name, "MariaDB source metadata");
        (void)rawCdcUint(field.value(), "source.pos");
      } else if (name == "row") {
        rawCdcMarkField(seen, row_bit, name, "MariaDB source metadata");
        (void)rawCdcUint(field.value(), "source.row");
      } else if (name == "thread") {
        rawCdcMarkField(seen, thread_bit, name, "MariaDB source metadata");
        rawCdcRequireNullableInteger(field.value(), "source.thread");
      } else if (name == "query") {
        rawCdcMarkField(seen, query_bit, name, "MariaDB source metadata");
        rawCdcRequireNullableString(field.value(), "source.query");
      } else {
        rawCdcMalformed("MariaDB source metadata contains unexpected field " +
                        std::string(name));
      }
    }
    rawCdcRequireFields(seen, expected, expected, "MariaDB source metadata");
    if (!identity_matches) {
      decodeError(CdcErrorCode::CdcRecordMalformed,
                  "raw CDC source/version identity is unsupported");
    }
    if (!snapshot_is_false) {
      decodeError(CdcErrorCode::ExternalWriterObserved,
                  "snapshot row is outside the captured live run");
    }
    return canonicalTableFromLeaf(table);
  }

  struct RawMariaDbTransaction {
    std::string id;
    std::uint64_t total_order = 0U;
    std::uint64_t data_collection_order = 0U;
  };

  [[nodiscard]] static RawMariaDbTransaction
  decodeRawMariaDbTransaction(simdjson::ondemand::value value) {
    constexpr std::uint32_t id_bit = 1U << 0U;
    constexpr std::uint32_t total_bit = 1U << 1U;
    constexpr std::uint32_t collection_bit = 1U << 2U;
    constexpr std::uint32_t expected = id_bit | total_bit | collection_bit;
    std::uint32_t seen = 0U;
    RawMariaDbTransaction result;
    for (auto field : value.get_object()) {
      const auto name = std::string_view(field.unescaped_key());
      if (name == "id") {
        rawCdcMarkField(seen, id_bit, name, "row transaction metadata");
        result.id = rawCdcString(field.value(), "row transaction.id");
      } else if (name == "total_order") {
        rawCdcMarkField(seen, total_bit, name, "row transaction metadata");
        result.total_order =
            rawCdcUint(field.value(), "row transaction.total_order");
      } else if (name == "data_collection_order") {
        rawCdcMarkField(seen, collection_bit, name, "row transaction metadata");
        result.data_collection_order =
            rawCdcUint(field.value(), "row transaction.data_collection_order");
      } else {
        rawCdcMalformed("row transaction metadata contains unexpected field " +
                        std::string(name));
      }
    }
    rawCdcRequireFields(seen, expected, expected, "row transaction metadata");
    return result;
  }

  [[nodiscard]] static Json
  decodeRawMariaDbRowKey(const std::string_view key_json) {
    auto &state = rawCdcParserState();
    state.input.assign(key_json);
    auto document = state.parser.iterate(state.input);
    auto key = rawCdcMaterialize(document.get_value());
    if (!key.is_object()) {
      rawCdcMalformed("row key is not an object");
    }
    if (key.empty()) {
      throw BackendCdcDecodeError(CdcErrorCode::PrimaryKeyImageIncomplete,
                                  "row key is empty");
    }
    return key;
  }

  [[nodiscard]] BackendCdcRecord
  decodeRawMariaDbRow(const std::string_view key_json,
                      const std::string_view envelope_json) const {
    auto key = decodeRawMariaDbRowKey(key_json);

    constexpr std::uint32_t after_bit = 1U << 0U;
    constexpr std::uint32_t source_bit = 1U << 1U;
    constexpr std::uint32_t transaction_bit = 1U << 2U;
    constexpr std::uint32_t operation_bit = 1U << 3U;
    constexpr std::uint32_t timestamp_ms_bit = 1U << 4U;
    constexpr std::uint32_t timestamp_us_bit = 1U << 5U;
    constexpr std::uint32_t timestamp_ns_bit = 1U << 6U;
    constexpr std::uint32_t before_bit = 1U << 7U;
    constexpr std::uint32_t required =
        after_bit | source_bit | transaction_bit | operation_bit |
        timestamp_ms_bit | timestamp_us_bit | timestamp_ns_bit;
    constexpr std::uint32_t allowed = required | before_bit;
    std::uint32_t seen = 0U;
    Json after;
    std::string canonical_table;
    RawMariaDbTransaction transaction;
    std::string operation;

    auto &state = rawCdcParserState();
    state.input.assign(envelope_json);
    auto document = state.parser.iterate(state.input);
    for (auto field : document.get_object()) {
      const auto name = std::string_view(field.unescaped_key());
      if (name == "after") {
        rawCdcMarkField(seen, after_bit, name, "MariaDB row envelope");
        after = rawCdcMaterialize(field.value());
        if (!after.is_null() && !after.is_object()) {
          rawCdcMalformed("row final image has an unsupported type");
        }
      } else if (name == "before") {
        rawCdcMarkField(seen, before_bit, name, "MariaDB row envelope");
        auto value = field.value().value();
        const auto type = value.type().value();
        if (type != simdjson::ondemand::json_type::null &&
            type != simdjson::ondemand::json_type::object) {
          rawCdcMalformed("row before image has an unsupported type");
        }
        rawCdcConsume(value);
      } else if (name == "source") {
        rawCdcMarkField(seen, source_bit, name, "MariaDB row envelope");
        canonical_table = decodeRawMariaDbSource(field.value());
      } else if (name == "transaction") {
        rawCdcMarkField(seen, transaction_bit, name, "MariaDB row envelope");
        transaction = decodeRawMariaDbTransaction(field.value());
      } else if (name == "op") {
        rawCdcMarkField(seen, operation_bit, name, "MariaDB row envelope");
        operation = rawCdcString(field.value(), "row operation", 1U);
      } else if (name == "ts_ms") {
        rawCdcMarkField(seen, timestamp_ms_bit, name, "MariaDB row envelope");
        (void)rawCdcUint(field.value(), "row envelope.ts_ms");
      } else if (name == "ts_us") {
        rawCdcMarkField(seen, timestamp_us_bit, name, "MariaDB row envelope");
        (void)rawCdcUint(field.value(), "row envelope.ts_us");
      } else if (name == "ts_ns") {
        rawCdcMarkField(seen, timestamp_ns_bit, name, "MariaDB row envelope");
        (void)rawCdcUint(field.value(), "row envelope.ts_ns");
      } else {
        rawCdcMalformed("MariaDB row envelope contains unexpected field " +
                        std::string(name));
      }
    }
    rawCdcRequireFields(seen, required, allowed, "MariaDB row envelope");
    if (operation != "c" && operation != "u" && operation != "d") {
      decodeError(CdcErrorCode::CdcRecordMalformed,
                  "row operation is unsupported");
    }

    BackendCdcRecord result;
    result.kind = BackendCdcRecordKind::Row;
    result.diagnostic_connector_id = std::move(transaction.id);
    result.canonical_table = std::move(canonical_table);
    result.operation = std::move(operation);
    result.key = std::move(key);
    result.after = std::move(after);
    result.total_order = transaction.total_order;
    result.data_collection_order = transaction.data_collection_order;
    return result;
  }

  [[nodiscard]] BackendCdcRecord decodeMetadata(const Json &key,
                                                const Json &envelope) const {
    requireExactKeys(key, {"id"}, "transaction metadata key");
    requireExactKeys(
        envelope, {"status", "id", "event_count", "data_collections", "ts_ms"},
        "transaction metadata envelope");
    const auto key_id =
        stringValue(member(key, "id", "transaction metadata key"),
                    "transaction metadata key.id");
    const auto envelope_id =
        stringValue(member(envelope, "id", "transaction metadata envelope"),
                    "transaction metadata envelope.id");
    if (key_id != envelope_id) {
      decodeError(CdcErrorCode::CdcTransactionIdMismatch,
                  "transaction metadata key and envelope IDs disagree");
    }
    (void)uintValue(member(envelope, "ts_ms", "transaction metadata envelope"),
                    "transaction metadata ts_ms");
    const auto status =
        stringValue(member(envelope, "status", "transaction metadata envelope"),
                    "transaction metadata status", 16U);
    BackendCdcRecord result;
    result.diagnostic_connector_id = envelope_id;
    if (status == "BEGIN") {
      if (!member(envelope, "event_count", "BEGIN").is_null() ||
          !member(envelope, "data_collections", "BEGIN").is_null()) {
        decodeError(CdcErrorCode::CdcRecordMalformed,
                    "BEGIN counts are not null");
      }
      result.kind = BackendCdcRecordKind::Begin;
      return result;
    }
    if (status != "END") {
      decodeError(CdcErrorCode::CdcRecordMalformed,
                  "transaction metadata status is unsupported");
    }
    result.kind = BackendCdcRecordKind::End;
    result.event_count =
        uintValue(member(envelope, "event_count", "END"), "END.event_count");
    const auto &collections = member(envelope, "data_collections", "END");
    if (!collections.is_array()) {
      decodeError(CdcErrorCode::CdcEventCountMismatch,
                  "END data_collections is not an array");
    }
    for (const auto &collection : collections) {
      requireExactKeys(collection, {"data_collection", "event_count"},
                       "END data collection");
      const auto identity = stringValue(
          member(collection, "data_collection", "END data collection"),
          "END data_collection");
      result.data_collections.push_back(BackendCdcDataCollectionCount{
          .canonical_table = canonicalDataCollection(identity),
          .event_count = uintValue(
              member(collection, "event_count", "END data collection"),
              "END data_collection.event_count"),
      });
    }
    return result;
  }

  [[nodiscard]] std::string validateSource(const Json &source) const {
    if (profile_ == Profile::MariaDb) {
      requireExactKeys(source,
                       {"version", "connector", "name", "ts_ms", "snapshot",
                        "db", "sequence", "ts_us", "ts_ns", "table",
                        "server_id", "gtid", "file", "pos", "row", "thread",
                        "query"},
                       "MariaDB source metadata");
    } else if (profile_ == Profile::YugabytePgoutput) {
      requireExactKeys(source,
                       {"version", "connector", "name", "ts_ms", "snapshot",
                        "db", "sequence", "schema", "table", "txId", "lsn",
                        "xmin", "origin", "origin_lsn"},
                       "YugabyteDB source metadata");
    } else {
      requireExactKeys(
          source, {"version",  "connector",    "name",         "ts_ms",
                   "snapshot", "db",           "sequence",     "ts_us",
                   "ts_ns",    "schema",       "table",        "txId",
                   "scn",      "commit_scn",   "lcr_position", "rs_id",
                   "ssn",      "redo_thread",  "user_name",    "redo_sql",
                   "row_id",   "commit_ts_ms", "start_scn",    "start_ts_ms",
                   "txSeq"},
          "Oracle source metadata");
    }
    const auto version =
        stringValue(member(source, "version", "source"), "source.version");
    const auto connector =
        stringValue(member(source, "connector", "source"), "source.connector");
    const auto source_name =
        stringValue(member(source, "name", "source"), "source.name", 256U);
    const auto database =
        stringValue(member(source, "db", "source"), "source.db", 64U);
    const auto expected_database =
        profile_ == Profile::OracleLogMiner ? "FREEPDB1" : "benchbase";
    const auto expected_connector = profile_ == Profile::MariaDb ? "mariadb"
                                    : profile_ == Profile::OracleLogMiner
                                        ? "oracle"
                                        : "postgresql";
    if (version != connector_version_ || source_name != source_id_ ||
        database != expected_database || connector != expected_connector) {
      decodeError(CdcErrorCode::CdcRecordMalformed,
                  "raw CDC source/version identity is unsupported");
    }
    if (!snapshotIsFalse(member(source, "snapshot", "source"))) {
      decodeError(CdcErrorCode::ExternalWriterObserved,
                  "snapshot row is outside the captured live run");
    }
    (void)uintValue(member(source, "ts_ms", "source"), "source.ts_ms");
    const auto table =
        stringValue(member(source, "table", "source"), "source.table", 64U);
    if (profile_ == Profile::MariaDb) {
      if (!member(source, "sequence", "source").is_null()) {
        decodeError(CdcErrorCode::CdcRecordMalformed,
                    "MariaDB source.sequence is unexpectedly present");
      }
      (void)uintValue(member(source, "ts_us", "source"), "source.ts_us");
      (void)uintValue(member(source, "ts_ns", "source"), "source.ts_ns");
      (void)uintValue(member(source, "server_id", "source"),
                      "source.server_id");
      requireNullableString(member(source, "gtid", "source"), "source.gtid");
      (void)stringValue(member(source, "file", "source"), "source.file");
      (void)uintValue(member(source, "pos", "source"), "source.pos");
      (void)uintValue(member(source, "row", "source"), "source.row");
      requireNullableInteger(member(source, "thread", "source"),
                             "source.thread");
      requireNullableString(member(source, "query", "source"), "source.query");
    } else if (profile_ == Profile::YugabytePgoutput) {
      const auto schema =
          stringValue(member(source, "schema", "source"), "source.schema", 64U);
      if (schema != "benchbase") {
        decodeError(CdcErrorCode::UnexpectedTable,
                    "YugabyteDB source schema is not benchbase");
      }
      (void)stringValue(member(source, "sequence", "source"),
                        "source.sequence");
      (void)uintValue(member(source, "txId", "source"), "source.txId");
      (void)uintValue(member(source, "lsn", "source"), "source.lsn");
      requireNullableInteger(member(source, "xmin", "source"), "source.xmin");
      requireNullableString(member(source, "origin", "source"),
                            "source.origin");
      requireNullableInteger(member(source, "origin_lsn", "source"),
                             "source.origin_lsn");
    } else {
      const auto schema = stringValue(member(source, "schema", "source"),
                                      "source.schema", 128U);
      if (schema != "BLACKBOX_OWNER") {
        decodeError(CdcErrorCode::UnexpectedTable,
                    "Oracle source schema is not BLACKBOX_OWNER");
      }
      if (!member(source, "sequence", "source").is_null()) {
        decodeError(CdcErrorCode::CdcRecordMalformed,
                    "Oracle source.sequence is unexpectedly present");
      }
      (void)uintValue(member(source, "ts_us", "source"), "source.ts_us");
      (void)uintValue(member(source, "ts_ns", "source"), "source.ts_ns");
      for (const auto field : {"txId", "scn", "commit_scn", "rs_id",
                               "user_name", "row_id", "start_scn"}) {
        (void)stringValue(member(source, field, "source"),
                          std::string("source.") + field, 512U);
      }
      requireNullableString(member(source, "lcr_position", "source"),
                            "source.lcr_position");
      requireNullableString(member(source, "redo_sql", "source"),
                            "source.redo_sql");
      for (const auto field :
           {"ssn", "redo_thread", "commit_ts_ms", "start_ts_ms", "txSeq"}) {
        (void)uintValue(member(source, field, "source"),
                        std::string("source.") + field);
      }
    }
    return canonicalTableFromLeaf(table);
  }

  [[nodiscard]] BackendCdcRecord decodeRow(const Json &key,
                                           const Json &envelope) const {
    if (profile_ != Profile::YugabytePgoutput) {
      requireKeys(
          envelope,
          {"after", "source", "transaction", "op", "ts_ms", "ts_us", "ts_ns"},
          {"before"}, "MariaDB row envelope");
    } else {
      requireKeys(envelope, {"after", "source", "transaction", "op", "ts_ms"},
                  {"before"}, "YugabyteDB row envelope");
    }
    requireObject(key, "row key");
    if (key.empty()) {
      decodeError(CdcErrorCode::PrimaryKeyImageIncomplete, "row key is empty");
    }
    const auto operation = stringValue(member(envelope, "op", "row envelope"),
                                       "row operation", 1U);
    if (operation != "c" && operation != "u" && operation != "d") {
      decodeError(CdcErrorCode::CdcRecordMalformed,
                  "row operation is unsupported");
    }
    const auto &after = member(envelope, "after", "row envelope");
    const auto before = envelope.find("before");
    if ((before != envelope.end() &&
         !(before->is_null() || before->is_object())) ||
        !(after.is_null() || after.is_object())) {
      decodeError(CdcErrorCode::CdcRecordMalformed,
                  "row before/final image has an unsupported type");
    }
    if (profile_ == Profile::YugabytePgoutput &&
        (containsSetWrapper(key) ||
         (before != envelope.end() && containsSetWrapper(*before)) ||
         containsSetWrapper(after))) {
      decodeError(CdcErrorCode::PrimaryKeyImageIncomplete,
                  "yboutput availability wrapper is not valid pgoutput data");
    }
    const auto &transaction = member(envelope, "transaction", "row envelope");
    requireExactKeys(transaction,
                     {"id", "total_order", "data_collection_order"},
                     "row transaction metadata");
    const auto diagnostic_id = stringValue(
        member(transaction, "id", "row transaction"), "row transaction.id");
    (void)uintValue(member(envelope, "ts_ms", "row envelope"),
                    "row envelope.ts_ms");
    if (profile_ != Profile::YugabytePgoutput) {
      (void)uintValue(member(envelope, "ts_us", "row envelope"),
                      "row envelope.ts_us");
      (void)uintValue(member(envelope, "ts_ns", "row envelope"),
                      "row envelope.ts_ns");
    }
    BackendCdcRecord result;
    result.kind = BackendCdcRecordKind::Row;
    result.diagnostic_connector_id = diagnostic_id;
    result.canonical_table =
        validateSource(member(envelope, "source", "row envelope"));
    result.operation = operation;
    result.key = profile_ == Profile::OracleLogMiner
                     ? canonicalOracleFields(key, result.canonical_table)
                     : key;
    result.after = profile_ == Profile::OracleLogMiner && after.is_object()
                       ? canonicalOracleFields(after, result.canonical_table)
                       : after;
    result.total_order =
        uintValue(member(transaction, "total_order", "row transaction"),
                  "row transaction.total_order");
    result.data_collection_order = uintValue(
        member(transaction, "data_collection_order", "row transaction"),
        "row transaction.data_collection_order");
    return result;
  }

  [[nodiscard]] Json
  canonicalOracleFields(const Json &object,
                        const std::string_view canonical_table) const {
    requireObject(object, "Oracle row object");
    Json result = Json::object();
    for (const auto &[name, value] : object.items()) {
      if (name.empty() || name.size() > 128U || name.front() < 'A' ||
          name.front() > 'Z' ||
          !std::all_of(name.begin() + 1U, name.end(), [](const char character) {
            return (character >= 'A' && character <= 'Z') ||
                   (character >= '0' && character <= '9') || character == '_';
          })) {
        decodeError(CdcErrorCode::CdcRecordMalformed,
                    "Oracle column name is not uppercase canonical text");
      }
      std::string canonical(name);
      std::transform(canonical.begin(), canonical.end(), canonical.begin(),
                     [](const unsigned char character) {
                       return static_cast<char>(std::tolower(character));
                     });
      if (result.contains(canonical)) {
        decodeError(CdcErrorCode::CdcRecordMalformed,
                    "Oracle column mapping is ambiguous");
      }
      result[canonical] = value;
    }
    const auto contract = oracle_contracts_.find(std::string(canonical_table));
    if (contract != oracle_contracts_.end()) {
      for (auto &[name, value] : result.items()) {
        const auto column = std::find_if(
            contract->second->columns.begin(), contract->second->columns.end(),
            [&name](const AdapterColumnContract &candidate) {
              return candidate.name == name;
            });
        if (column == contract->second->columns.end()) {
          decodeError(CdcErrorCode::CdcRecordMalformed,
                      "Oracle row contains an unregistered column");
        }
        normalizeOracleValue(value, *column);
      }
    } else if (canonical_table == "benchbase.session_txn") {
      normalizeOracleSignedNumber(result, "txn_id", false);
    }
    return result;
  }

  static void normalizeOracleSignedNumber(Json &object,
                                          const std::string_view field,
                                          const bool i32) {
    const auto found = object.find(field);
    if (found == object.end() || !found->is_string()) {
      decodeError(CdcErrorCode::CdcRecordMalformed,
                  "Oracle control NUMBER is not canonical text");
    }
    const auto text = found->get<std::string>();
    std::int64_t value = 0;
    const auto parsed =
        std::from_chars(text.data(), text.data() + text.size(), value);
    if (text.empty() || parsed.ec != std::errc{} ||
        parsed.ptr != text.data() + text.size() ||
        (text.size() > 1U && text.front() == '0') || text == "-0" ||
        (text.starts_with('-') && text.size() > 2U && text[1] == '0') ||
        (i32 && (value < std::numeric_limits<std::int32_t>::min() ||
                 value > std::numeric_limits<std::int32_t>::max()))) {
      decodeError(CdcErrorCode::CdcRecordMalformed,
                  "Oracle control NUMBER text is noncanonical or out of range");
    }
    *found = value;
  }

  static void normalizeOracleValue(Json &value,
                                   const AdapterColumnContract &column) {
    if (value.is_null()) {
      if (!column.nullable) {
        decodeError(CdcErrorCode::CdcRecordMalformed,
                    "Oracle non-null column is null");
      }
      return;
    }
    if (column.canonical_type == "i32" || column.canonical_type == "i64") {
      Json wrapper{{std::string(column.name), value}};
      normalizeOracleSignedNumber(wrapper, column.name,
                                  column.canonical_type == "i32");
      value = std::move(wrapper[std::string(column.name)]);
      return;
    }
    if (column.canonical_type == "fixed_decimal") {
      if (!value.is_string()) {
        decodeError(CdcErrorCode::CdcRecordMalformed,
                    "Oracle fixed decimal is not string encoded");
      }
      return;
    }
    if (column.canonical_type == "float32") {
      if (value.is_number()) {
        return;
      }
      if (!value.is_string()) {
        decodeError(CdcErrorCode::CdcRecordMalformed,
                    "Oracle BINARY_FLOAT has an unsupported JSON type");
      }
      const auto text = value.get<std::string>();
      double number = 0;
      const auto parsed =
          std::from_chars(text.data(), text.data() + text.size(), number);
      if (text.empty() || parsed.ec != std::errc{} ||
          parsed.ptr != text.data() + text.size() || !std::isfinite(number) ||
          !std::isfinite(static_cast<float>(number))) {
        decodeError(CdcErrorCode::CdcRecordMalformed,
                    "Oracle BINARY_FLOAT text is malformed or non-finite");
      }
      value = number;
      return;
    }
    if (column.canonical_type == "timestamp_utc") {
      const auto milliseconds = uintValue(value, "Oracle timestamp");
      if (milliseconds % 1000U != 0U ||
          milliseconds / 1000U > static_cast<std::uint64_t>(
                                     std::numeric_limits<std::time_t>::max())) {
        decodeError(CdcErrorCode::CdcRecordMalformed,
                    "Oracle timestamp is not an exact supported UTC second");
      }
      const auto seconds = static_cast<std::time_t>(milliseconds / 1000U);
      std::tm utc{};
      if (gmtime_r(&seconds, &utc) == nullptr) {
        decodeError(CdcErrorCode::CdcRecordMalformed,
                    "Oracle timestamp cannot be converted to UTC");
      }
      std::array<char, 21> rendered{};
      if (std::strftime(rendered.data(), rendered.size(), "%Y-%m-%dT%H:%M:%SZ",
                        &utc) != 20U) {
        decodeError(CdcErrorCode::CdcRecordMalformed,
                    "Oracle timestamp UTC rendering failed");
      }
      value = std::string(rendered.data(), 20U);
      return;
    }
    if ((column.canonical_type == "utf8_varchar" ||
         column.canonical_type == "utf8_char") &&
        value.is_string()) {
      return;
    }
    decodeError(CdcErrorCode::CdcRecordMalformed,
                "Oracle column has an unsupported canonical representation");
  }

  std::string source_id_;
  std::string connector_version_;
  Profile profile_;
  std::unordered_set<std::string> canonical_tables_;
  std::unordered_map<std::string, std::string> canonical_by_leaf_;
  std::unordered_map<std::string, const AdapterTableContract *>
      oracle_contracts_;
};

constexpr auto mariadb_fact_contract = std::to_array<BackendFactRule>({
    {"database_name", BackendFactValueRule::Exact, "benchbase"},
    {"schema_name", BackendFactValueRule::Exact, "benchbase"},
    {"database_character_set", BackendFactValueRule::Exact, "utf8mb4"},
    {"database_collation", BackendFactValueRule::Exact, "utf8mb4_bin"},
    {"application_table_engine", BackendFactValueRule::Exact, "InnoDB"},
    {"application_table_character_set", BackendFactValueRule::Exact, "utf8mb4"},
    {"application_table_collation", BackendFactValueRule::Exact, "utf8mb4_bin"},
    {"server_character_set", BackendFactValueRule::Exact, "utf8mb4"},
    {"server_collation", BackendFactValueRule::Exact, "utf8mb4_bin"},
    {"server_time_zone", BackendFactValueRule::Exact, "+00:00"},
    {"server_sql_mode", BackendFactValueRule::Exact, pinned_sql_mode},
    {"server_transaction_isolation", BackendFactValueRule::Exact,
     "REPEATABLE_READ"},
    {"server_snapshot_isolation", BackendFactValueRule::Exact, "true"},
    {"connector_class", BackendFactValueRule::Exact,
     "io.debezium.connector.mariadb.MariaDbConnector"},
    {"connector_source", BackendFactValueRule::Exact, "mariadb"},
    {"connector_version", BackendFactValueRule::Exact, "3.6.1.Final"},
    {"connector_tasks", BackendFactValueRule::Exact, "1"},
    {"redis_stream", BackendFactValueRule::Exact, "cdc"},
    {"database_image_digest", BackendFactValueRule::Sha256, {}},
    {"capture_table_roster_sha256", BackendFactValueRule::Sha256, {}},
    {"table_definition_sha256", BackendFactValueRule::Sha256, {}},
    {"dedicated_privileges_sha256", BackendFactValueRule::Sha256, {}},
});

constexpr auto yugabyte_fact_contract = std::to_array<BackendFactRule>({
    {"database_name", BackendFactValueRule::Exact, "benchbase"},
    {"schema_name", BackendFactValueRule::Exact, "benchbase"},
    {"server_version", BackendFactValueRule::Exact, "2025.2.5.2-b5"},
    {"server_build_hash", BackendFactValueRule::Exact,
     "84b2d0064618859f160408c0ce1cb33985581943"},
    {"ysql_server_version", BackendFactValueRule::Exact,
     "PostgreSQL 15.12-YB-2025.2.5.2-b0 on x86_64-pc-linux-gnu, compiled by "
     "clang version 19.1.0 (https://github.com/yugabyte/llvm-project.git "
     "a2a6b655e14e7fa1fcf1011a6cb29cb8575249c0), 64-bit"},
    {"client_encoding", BackendFactValueRule::Exact, "UTF8"},
    {"time_zone", BackendFactValueRule::Exact, "UTC"},
    {"search_path", BackendFactValueRule::Exact, "benchbase"},
    {"default_transaction_isolation", BackendFactValueRule::Exact,
     "repeatable read"},
    {"yb_read_from_followers", BackendFactValueRule::Exact, "false"},
    {"publication_name", BackendFactValueRule::CanonicalIdentifier, {}},
    {"slot_name", BackendFactValueRule::CanonicalIdentifier, {}},
    {"slot_database", BackendFactValueRule::Exact, "benchbase"},
    {"slot_plugin", BackendFactValueRule::Exact, "pgoutput"},
    {"slot_lsn_type", BackendFactValueRule::Exact, "SEQUENCE"},
    {"streaming_mode", BackendFactValueRule::Exact, "default"},
    {"connector_class", BackendFactValueRule::Exact,
     "io.debezium.connector.postgresql.YugabyteDBConnector"},
    {"connector_source", BackendFactValueRule::Exact, "postgresql"},
    {"connector_version", BackendFactValueRule::Exact, "dz.2.5.2.yb.2026.1"},
    {"connector_sha256", BackendFactValueRule::Exact,
     "4e83132cbebdec53e553f69d4cfdbe48dac563105aabc5db4b880cdcf0118d98"},
    {"connector_tasks", BackendFactValueRule::Exact, "1"},
    {"redis_stream", BackendFactValueRule::Exact, "cdc"},
    {"physical_capture_profile", BackendFactValueRule::CanonicalIdentifier, {}},
    {"cdc_enable_intra_transactional_before_image",
     BackendFactValueRule::CanonicalIdentifier,
     {}},
    {"cdc_send_null_before_image_if_not_exists",
     BackendFactValueRule::CanonicalIdentifier,
     {}},
    {"database_image_digest", BackendFactValueRule::Sha256, {}},
    {"publication_membership_sha256", BackendFactValueRule::Sha256, {}},
    {"replica_identity_tables_sha256", BackendFactValueRule::Sha256, {}},
    {"table_oid_type_typmod_sha256", BackendFactValueRule::Sha256, {}},
    {"dedicated_privileges_sha256", BackendFactValueRule::Sha256, {}},
});

constexpr auto oracle_fact_contract = std::to_array<BackendFactRule>({
    {"database_name", BackendFactValueRule::Exact, "FREEPDB1"},
    {"schema_name", BackendFactValueRule::Exact, "BLACKBOX_OWNER"},
    {"server_version", BackendFactValueRule::Exact,
     "Oracle AI Database 26ai Free 23.26.2.0.0"},
    {"database_character_set", BackendFactValueRule::Exact, "AL32UTF8"},
    {"nls_comp", BackendFactValueRule::Exact, "BINARY"},
    {"nls_sort", BackendFactValueRule::Exact, "BINARY"},
    {"session_time_zone", BackendFactValueRule::Exact, "+00:00"},
    {"session_transaction_isolation", BackendFactValueRule::Exact,
     "SERIALIZABLE"},
    {"bootstrap_loader_isolation", BackendFactValueRule::Exact,
     "READ_COMMITTED"},
    {"archive_log_mode", BackendFactValueRule::Exact, "ARCHIVELOG"},
    {"force_logging", BackendFactValueRule::Exact, "YES"},
    {"supplemental_log_data_min", BackendFactValueRule::Exact, "YES"},
    {"online_redo_log_groups", BackendFactValueRule::Exact, "6"},
    {"online_redo_large_groups", BackendFactValueRule::Exact, "4"},
    {"online_redo_large_group_bytes", BackendFactValueRule::Exact, "536870912"},
    {"table_supplemental_logging", BackendFactValueRule::Exact, "ALL_COLUMNS"},
    {"log_mining_strategy", BackendFactValueRule::Exact, "online_catalog"},
    {"log_mining_batch_size_default", BackendFactValueRule::Exact, "20000"},
    {"log_mining_sleep_profile", BackendFactValueRule::Exact, "0_200_1000_200"},
    {"physical_capture_profile", BackendFactValueRule::Exact,
     "oracle_logminer_all_columns_v1"},
    {"connector_class", BackendFactValueRule::Exact,
     "io.debezium.connector.oracle.OracleConnector"},
    {"connector_source", BackendFactValueRule::Exact, "oracle"},
    {"connector_version", BackendFactValueRule::Exact, "3.6.1.Final"},
    {"connector_sha256", BackendFactValueRule::Exact,
     "b3a81a6d4e0822788a2c1a04bfd6007adcfedd0d07edeb6deebd8df73834fed8"},
    {"connector_ojdbc_sha256", BackendFactValueRule::Exact,
     "ef295bf01374dac7d791b3bc6526d10e8910ecac4645a95a3ca0c01cfc5c21d0"},
    {"benchbase_ojdbc_sha256", BackendFactValueRule::Exact,
     "ec8b7f2020b03b19f572e1bc34f94330610e86d3113ffe1e1f0474b8f5ce88ed"},
    {"connector_tasks", BackendFactValueRule::Exact, "1"},
    {"redis_stream", BackendFactValueRule::Exact, "cdc"},
    {"database_image_digest", BackendFactValueRule::Sha256, {}},
    {"capture_table_roster_sha256", BackendFactValueRule::Sha256, {}},
    {"table_definition_sha256", BackendFactValueRule::Sha256, {}},
    {"dedicated_privileges_sha256", BackendFactValueRule::Sha256, {}},
});

class MariaDbBackendAdapter final : public BackendAdapter {
public:
  [[nodiscard]] std::string_view name() const noexcept override {
    return "mariadb";
  }

  [[nodiscard]] std::string_view connectorVersion() const noexcept override {
    return "3.6.1.Final";
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

  [[nodiscard]] BackendNormalizedCapabilities
  normalizedCapabilities() const noexcept override {
    return BackendNormalizedCapabilities{.connector_startup_schema_evidence =
                                             true};
  }

  [[nodiscard]] std::span<const BackendFactRule>
  attestationFactContract() const noexcept override {
    return mariadb_fact_contract;
  }

  [[nodiscard]] std::unique_ptr<CdcStartupSchemaDecoder>
  createCdcStartupSchemaDecoder(CdcStartupSchemaConfig config) const override {
    return std::make_unique<MariaDbStartupSchemaDecoder>(std::move(config));
  }

  [[nodiscard]] std::unique_ptr<BackendCdcDecoder>
  createCdcDecoder(BackendCdcDecoderConfig config) const override {
    return std::make_unique<PinnedEnvelopeDecoder>(
        std::move(config), PinnedEnvelopeDecoder::Profile::MariaDb);
  }
};

class YugabyteYsqlBackendAdapter final : public BackendAdapter {
public:
  [[nodiscard]] std::string_view name() const noexcept override {
    return "yugabytedb_ysql";
  }

  [[nodiscard]] std::string_view connectorVersion() const noexcept override {
    return "dz.2.5.2.yb.2026.1";
  }

  [[nodiscard]] BackendServerDefaults serverDefaults() const noexcept override {
    return BackendServerDefaults{
        .character_set = "UTF8",
        .collation = "C",
        .time_zone = "UTC",
        .sql_mode = {},
        .transaction_isolation = "REPEATABLE_READ",
        .snapshot_isolation = true,
    };
  }

  [[nodiscard]] BackendSessionRequirements
  sessionRequirements() const noexcept override {
    return BackendSessionRequirements{
        .set_names = "SET client_encoding TO 'UTF8'",
        .character_set_client = "UTF8",
        .character_set_connection = "UTF8",
        .character_set_results = "UTF8",
        .collation_connection = "C",
        .time_zone = "UTC",
        .sql_mode = {},
        .transaction_isolation = "REPEATABLE_READ",
        .snapshot_isolation = true,
    };
  }

  [[nodiscard]] BackendDatabaseDefaults
  databaseDefaults() const noexcept override {
    return BackendDatabaseDefaults{.character_set = "UTF8", .collation = "C"};
  }

  [[nodiscard]] ApplicationTableRequirements
  applicationTableRequirements() const noexcept override {
    return ApplicationTableRequirements{
        .engine = {},
        .character_set = "UTF8",
        .collation = "C",
        .char_read_semantics = "pad_to_declared_length",
    };
  }

  [[nodiscard]] BackendNormalizedCapabilities
  normalizedCapabilities() const noexcept override {
    return BackendNormalizedCapabilities{.connector_startup_schema_evidence =
                                             false};
  }

  [[nodiscard]] std::span<const BackendFactRule>
  attestationFactContract() const noexcept override {
    return yugabyte_fact_contract;
  }

  [[nodiscard]] std::unique_ptr<CdcStartupSchemaDecoder>
  createCdcStartupSchemaDecoder(CdcStartupSchemaConfig) const override {
    return std::make_unique<CatalogOnlyStartupSchemaDecoder>();
  }

  [[nodiscard]] std::unique_ptr<BackendCdcDecoder>
  createCdcDecoder(BackendCdcDecoderConfig config) const override {
    return std::make_unique<PinnedEnvelopeDecoder>(
        std::move(config), PinnedEnvelopeDecoder::Profile::YugabytePgoutput);
  }
};

class OracleBackendAdapter final : public BackendAdapter {
public:
  [[nodiscard]] std::string_view name() const noexcept override {
    return "oracle";
  }

  [[nodiscard]] std::string_view connectorVersion() const noexcept override {
    return "3.6.1.Final";
  }

  [[nodiscard]] BackendServerDefaults serverDefaults() const noexcept override {
    return BackendServerDefaults{
        .character_set = "AL32UTF8",
        .collation = "BINARY",
        .time_zone = "+00:00",
        .sql_mode = {},
        .transaction_isolation = "SERIALIZABLE",
        .snapshot_isolation = true,
    };
  }

  [[nodiscard]] BackendSessionRequirements
  sessionRequirements() const noexcept override {
    return BackendSessionRequirements{
        .set_names = "ALTER SESSION SET CURRENT_SCHEMA=BLACKBOX_OWNER",
        .character_set_client = "AL32UTF8",
        .character_set_connection = "AL32UTF8",
        .character_set_results = "AL32UTF8",
        .collation_connection = "BINARY",
        .time_zone = "+00:00",
        .sql_mode = {},
        .transaction_isolation = "SERIALIZABLE",
        .snapshot_isolation = true,
    };
  }

  [[nodiscard]] BackendDatabaseDefaults
  databaseDefaults() const noexcept override {
    return BackendDatabaseDefaults{.character_set = "AL32UTF8",
                                   .collation = "BINARY"};
  }

  [[nodiscard]] ApplicationTableRequirements
  applicationTableRequirements() const noexcept override {
    return ApplicationTableRequirements{
        .engine = {},
        .character_set = "AL32UTF8",
        .collation = "BINARY",
        .char_read_semantics = "pad_to_declared_length",
    };
  }

  [[nodiscard]] BackendNormalizedCapabilities
  normalizedCapabilities() const noexcept override {
    return BackendNormalizedCapabilities{.connector_startup_schema_evidence =
                                             false};
  }

  [[nodiscard]] std::span<const BackendFactRule>
  attestationFactContract() const noexcept override {
    return oracle_fact_contract;
  }

  [[nodiscard]] std::unique_ptr<CdcStartupSchemaDecoder>
  createCdcStartupSchemaDecoder(CdcStartupSchemaConfig) const override {
    return std::make_unique<CatalogOnlyStartupSchemaDecoder>();
  }

  [[nodiscard]] std::unique_ptr<BackendCdcDecoder>
  createCdcDecoder(BackendCdcDecoderConfig config) const override {
    return std::make_unique<PinnedEnvelopeDecoder>(
        std::move(config), PinnedEnvelopeDecoder::Profile::OracleLogMiner);
  }
};

const MariaDbBackendAdapter mariadb_adapter;
const YugabyteYsqlBackendAdapter yugabyte_ysql_adapter;
const OracleBackendAdapter oracle_adapter;
const std::array<const BackendAdapter *, 3> registered_adapters = {
    &mariadb_adapter,
    &yugabyte_ysql_adapter,
    &oracle_adapter,
};

} // namespace

BackendCdcRecord
BackendCdcDecoder::decodeRaw(const std::string_view key_json,
                             const std::string_view envelope_json) const {
  auto key = detail::parseStrictJson(key_json);
  auto envelope = detail::parseStrictJson(envelope_json);
  return decode(key, envelope);
}

void BackendAdapter::validateSchemaAttestation(
    const SchemaAttestation &attestation,
    const BenchmarkAdapter &benchmark_adapter) const {
  if (attestation.backend != name()) {
    throw MonitorError(MonitorErrorCode::SchemaAttestationMismatch,
                       "SchemaAttestation backend differs from adapter");
  }
  const auto find_table =
      [&](const std::string_view name) -> const ProtocolSchemaTable * {
    const auto found =
        std::find_if(attestation.tables.begin(), attestation.tables.end(),
                     [name](const ProtocolSchemaTable &table) {
                       return table.canonical_table_name == name;
                     });
    return found == attestation.tables.end() ? nullptr : &*found;
  };
  const auto validate_columns =
      [](const ProtocolSchemaTable &actual,
         const std::span<const AdapterColumnContract> expected) {
        if (actual.columns.size() != expected.size()) {
          return false;
        }
        for (std::size_t index = 0; index < expected.size(); ++index) {
          const auto &column = actual.columns[index];
          const auto &contract = expected[index];
          if (column.name != contract.name ||
              column.logical_sql_type != contract.sql_type ||
              column.nullable != contract.nullable ||
              column.generated != contract.generated.has_value()) {
            return false;
          }
        }
        return true;
      };
  if (attestation.tables.size() != benchmark_adapter.tables().size() + 2U) {
    throw MonitorError(MonitorErrorCode::SchemaAttestationMismatch,
                       "SchemaAttestation table roster size differs");
  }
  for (const auto &contract : benchmark_adapter.tables()) {
    const auto *table = find_table(contract.name);
    if (table == nullptr ||
        !std::equal(table->primary_key_columns.begin(),
                    table->primary_key_columns.end(),
                    contract.primary_key.begin(), contract.primary_key.end()) ||
        !validate_columns(*table, contract.columns)) {
      throw MonitorError(MonitorErrorCode::SchemaAttestationMismatch,
                         "SchemaAttestation application table differs from "
                         "benchmark contract");
    }
  }
  const auto *fence = find_table("benchbase.checker_run_fence");
  const auto *marker = find_table("benchbase.session_txn");
  const std::vector<std::string> fence_key = {"run_id", "phase"};
  const std::vector<std::string> marker_key = {"session_id", "txn_id"};
  const auto matches_column = [](const ProtocolSchemaColumn &column,
                                 const std::string_view name,
                                 const std::string_view type) {
    return column.name == name && column.logical_sql_type == type &&
           !column.nullable && !column.generated;
  };
  if (fence == nullptr || fence->primary_key_columns != fence_key ||
      fence->columns.size() != 3U ||
      !matches_column(fence->columns[0], "run_id", "CHAR(36)") ||
      !matches_column(fence->columns[1], "phase", "VARCHAR(32)") ||
      !matches_column(fence->columns[2], "nonce", "CHAR(36)") ||
      marker == nullptr || marker->primary_key_columns != marker_key ||
      marker->columns.size() != 2U ||
      !matches_column(marker->columns[0], "session_id", "VARCHAR(64)") ||
      !matches_column(marker->columns[1], "txn_id", "BIGINT")) {
    throw MonitorError(
        MonitorErrorCode::SchemaAttestationMismatch,
        "SchemaAttestation common control table contract differs");
  }

  const auto rules = attestationFactContract();
  if (attestation.backend_facts.size() != rules.size()) {
    throw MonitorError(MonitorErrorCode::BackendFactMismatch,
                       "SchemaAttestation backend fact count differs");
  }
  const auto is_sha256 = [](const std::string_view value) {
    return value.size() == 64U &&
           std::all_of(value.begin(), value.end(), [](const char character) {
             return (character >= '0' && character <= '9') ||
                    (character >= 'a' && character <= 'f');
           });
  };
  const auto is_positive_integer = [](const std::string_view value) {
    return !value.empty() && value.front() != '0' &&
           std::all_of(value.begin(), value.end(), [](const char character) {
             return character >= '0' && character <= '9';
           });
  };
  for (std::size_t index = 0; index < rules.size(); ++index) {
    const auto &fact = attestation.backend_facts[index];
    const auto &rule = rules[index];
    bool valid = fact.name == rule.name;
    switch (rule.value_rule) {
    case BackendFactValueRule::Exact:
      valid = valid && fact.canonical_value == rule.expected_value;
      break;
    case BackendFactValueRule::Sha256:
      valid = valid && is_sha256(fact.canonical_value);
      break;
    case BackendFactValueRule::CanonicalIdentifier:
      valid = valid && isCanonicalIdentifier(fact.canonical_value);
      break;
    case BackendFactValueRule::PositiveInteger:
      valid = valid && is_positive_integer(fact.canonical_value);
      break;
    }
    if (!valid) {
      throw MonitorError(
          MonitorErrorCode::BackendFactMismatch,
          "SchemaAttestation backend fact differs from contract");
    }
  }
}

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

BackendAdapterCatalog::BackendAdapterCatalog(
    const std::span<const BackendAdapter *const> adapters)
    : adapters_(adapters) {
  if (adapters_.empty()) {
    throw std::invalid_argument("backend adapter catalog is empty");
  }
  std::unordered_set<std::string_view> names;
  for (const auto *adapter : adapters_) {
    if (adapter == nullptr || !isCanonicalIdentifier(adapter->name()) ||
        !names.insert(adapter->name()).second) {
      throw std::invalid_argument("backend adapter catalog is invalid");
    }
  }
}

const BackendAdapter *
BackendAdapterCatalog::find(const std::string_view backend) const noexcept {
  const auto found =
      std::find_if(adapters_.begin(), adapters_.end(),
                   [backend](const BackendAdapter *adapter) {
                     return adapter != nullptr && adapter->name() == backend;
                   });
  return found == adapters_.end() ? nullptr : *found;
}

const BackendAdapter &
BackendAdapterCatalog::require(const std::string_view backend) const {
  const auto *adapter = find(backend);
  if (adapter == nullptr) {
    throw MonitorError(MonitorErrorCode::ConfigInvalid,
                       "backend adapter catalog has no requested registration");
  }
  return *adapter;
}

} // namespace blackbox
