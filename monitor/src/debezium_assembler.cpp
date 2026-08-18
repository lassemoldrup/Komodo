#include "blackbox/debezium_assembler.h"

#include "blackbox/canonical_ycsb_codec.h"
#include "strict_json.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace blackbox {
namespace {

using Json = nlohmann::json;

constexpr std::uint64_t kMaxJsonSafeInteger = 9007199254740991ULL;

struct StreamId {
  std::uint64_t milliseconds = 0;
  std::uint64_t sequence = 0;

  auto operator<=>(const StreamId &) const = default;
};

[[nodiscard]] bool hasCanonicalDecimalSpelling(const std::string_view text) {
  if (text.empty()) {
    return false;
  }
  if (text.size() > 1U && text.front() == '0') {
    return false;
  }
  return std::all_of(text.begin(), text.end(), [](const char value) {
    return value >= '0' && value <= '9';
  });
}

[[nodiscard]] std::optional<std::uint64_t>
parseUint64Text(const std::string_view text) {
  if (!hasCanonicalDecimalSpelling(text)) {
    return std::nullopt;
  }
  std::uint64_t result = 0;
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), result);
  if (error != std::errc{} || end != text.data() + text.size()) {
    return std::nullopt;
  }
  return result;
}

[[nodiscard]] std::optional<StreamId>
parseStreamId(const std::string_view value) {
  const auto separator = value.find('-');
  if (separator == std::string_view::npos ||
      value.find('-', separator + 1U) != std::string_view::npos) {
    return std::nullopt;
  }
  const auto milliseconds = parseUint64Text(value.substr(0, separator));
  const auto sequence = parseUint64Text(value.substr(separator + 1U));
  if (!milliseconds.has_value() || !sequence.has_value()) {
    return std::nullopt;
  }
  return StreamId{.milliseconds = *milliseconds, .sequence = *sequence};
}

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

[[nodiscard]] bool isLowerUuid(const std::string_view value) {
  if (value.size() != 36U) {
    return false;
  }
  constexpr std::array<std::size_t, 4> hyphens{8U, 13U, 18U, 23U};
  for (std::size_t index = 0; index < value.size(); ++index) {
    if (std::find(hyphens.begin(), hyphens.end(), index) != hyphens.end()) {
      if (value[index] != '-') {
        return false;
      }
      continue;
    }
    const auto character = value[index];
    if (!((character >= '0' && character <= '9') ||
          (character >= 'a' && character <= 'f'))) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] bool isSessionId(const std::string_view value) {
  if (value.empty() || value.size() > 256U) {
    return false;
  }
  const auto first = value.front();
  if (!((first >= 'A' && first <= 'Z') || (first >= 'a' && first <= 'z') ||
        (first >= '0' && first <= '9'))) {
    return false;
  }
  return std::all_of(value.begin() + 1, value.end(), [](const char character) {
    return (character >= 'A' && character <= 'Z') ||
           (character >= 'a' && character <= 'z') ||
           (character >= '0' && character <= '9') || character == '.' ||
           character == '_' || character == ':' || character == '/' ||
           character == '-';
  });
}

struct QualifiedTable {
  std::string schema;
  std::string table;
  std::string full_name;
};

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

struct CanonicalBytesLess {
  bool operator()(const CanonicalBytes &left,
                  const CanonicalBytes &right) const noexcept {
    return std::lexicographical_compare(
        left.begin(), left.end(), right.begin(), right.end(),
        [](const std::byte lhs, const std::byte rhs) {
          return std::to_integer<unsigned int>(lhs) <
                 std::to_integer<unsigned int>(rhs);
        });
  }
};

} // namespace

class DebeziumTxnAssembler::Impl final {
public:
  explicit Impl(DebeziumAssemblerConfig config) : config_(std::move(config)) {
    validateConfig();
  }

  [[nodiscard]] std::optional<CdcTransaction>
  accept(const CdcStreamRecord &record) {
    if (failure_.has_value()) {
      throw CdcAssemblerError(failure_->code, failure_->message,
                              failure_->stream_id);
    }

    current_stream_id_ = record.stream_id;
    if (finished_) {
      const CdcAssemblerError error(
          CdcErrorCode::CdcOrderInvalid,
          "CDC evidence was supplied after end-of-input was declared",
          record.stream_id);
      rememberFailure(error);
      throw error;
    }
    try {
      if (record.key_json.size() > config_.max_active_bytes ||
          record.envelope_json.size() >
              config_.max_active_bytes - record.key_json.size()) {
        fail(CdcErrorCode::ResourceExhausted,
             "one CDC record exceeds the configured assembly byte limit");
      }
      validateStreamOrder(record);
      const auto key = detail::parseStrictJson(record.key_json);
      const auto envelope = detail::parseStrictJson(record.envelope_json);
      if (!envelope.is_object()) {
        fail(CdcErrorCode::CdcRecordMalformed,
             "Debezium envelope is not a JSON object");
      }
      const bool has_status = envelope.contains("status");
      const bool has_operation = envelope.contains("op");
      if (has_status == has_operation) {
        if (!has_status && envelope.contains("ddl")) {
          acceptStartupSchemaSnapshot(key, envelope);
          recordAcceptedRaw(record);
          return std::nullopt;
        }
        fail(CdcErrorCode::CdcRecordMalformed,
             "Debezium record must be exactly one of transaction metadata or "
             "row data");
      }
      if (has_status) {
        auto completed = acceptTransactionMetadata(record, key, envelope);
        recordAcceptedRaw(record);
        return completed;
      }
      acceptRow(record, key, envelope);
      recordAcceptedRaw(record);
      return std::nullopt;
    } catch (const CdcAssemblerError &error) {
      rememberFailure(error);
      throw;
    } catch (const std::bad_alloc &) {
      const CdcAssemblerError error(
          CdcErrorCode::ResourceExhausted,
          "allocation failed while assembling CDC evidence", record.stream_id);
      rememberFailure(error);
      throw error;
    } catch (const detail::StrictJsonError &error) {
      const CdcAssemblerError wrapped(
          CdcErrorCode::CdcRecordMalformed,
          std::string("invalid or ambiguous CDC JSON: ") + error.what(),
          record.stream_id);
      rememberFailure(wrapped);
      throw wrapped;
    } catch (const std::exception &error) {
      const CdcAssemblerError wrapped(
          CdcErrorCode::CdcRecordMalformed,
          std::string("failed to decode CDC record: ") + error.what(),
          record.stream_id);
      rememberFailure(wrapped);
      throw wrapped;
    }
  }

  void finish() {
    if (failure_.has_value()) {
      throw CdcAssemblerError(failure_->code, failure_->message,
                              failure_->stream_id);
    }
    if (!active_.empty()) {
      const auto &transaction = active_.begin()->second;
      const CdcAssemblerError error(
          CdcErrorCode::CdcEndMissing,
          "end of input reached with an incomplete Debezium transaction",
          transaction.begin_stream_id);
      rememberFailure(error);
      throw error;
    }
    if (!saw_startup_schema_snapshot_) {
      const CdcAssemblerError error(
          CdcErrorCode::CdcRecordMalformed,
          "end of input reached without the required startup schema snapshot",
          current_stream_id_);
      rememberFailure(error);
      throw error;
    }
    if (!startup_schema_snapshot_finished_) {
      const CdcAssemblerError error(
          CdcErrorCode::DdlObserved,
          "end of input reached before the startup schema snapshot's last "
          "record",
          current_stream_id_);
      rememberFailure(error);
      throw error;
    }
    finished_ = true;
  }

  [[nodiscard]] bool failed() const noexcept { return failure_.has_value(); }

  [[nodiscard]] std::optional<CdcErrorCode> errorCode() const noexcept {
    if (!failure_.has_value()) {
      return std::nullopt;
    }
    return failure_->code;
  }

  [[nodiscard]] std::uint64_t completedCount() const noexcept {
    return next_cdc_seq_;
  }

  [[nodiscard]] std::size_t activeTransactionCount() const noexcept {
    return active_.size();
  }

  [[nodiscard]] std::size_t activeBytes() const noexcept {
    return active_bytes_;
  }

  [[nodiscard]] std::size_t retainedIdentityBytes() const noexcept {
    return retained_identity_bytes_;
  }

  [[nodiscard]] DebeziumAssemblerMetrics metrics() const noexcept {
    return DebeziumAssemblerMetrics{
        .accepted_raw_records = accepted_raw_records_,
        .accepted_raw_bytes = accepted_raw_bytes_,
        .startup_schema_records =
            static_cast<std::uint64_t>(startup_schema_snapshot_records_),
        .begin_metadata_records = begin_metadata_records_,
        .end_metadata_records = end_metadata_records_,
        .row_records = row_records_,
        .completed_transactions = next_cdc_seq_,
        .completed_run_control_transactions =
            completed_run_control_transactions_,
        .completed_bootstrap_transactions = completed_bootstrap_transactions_,
        .completed_workload_transactions = completed_workload_transactions_,
        .raw_row_events = raw_row_events_,
        .normalized_write_events = normalized_write_events_,
        .active_assembly_bytes = active_bytes_,
        .active_assembly_high_water_bytes = active_bytes_high_water_,
        .retained_identity_bytes = retained_identity_bytes_,
    };
  }

private:
  enum class LifecyclePhase {
    AwaitRunStart,
    Bootstrap,
    Workload,
    Closed,
  };

  enum class StartupSchemaStage {
    ServerSettings,
    DropTables,
    CreateDatabase,
    UseDatabase,
    CreateTables,
    Complete,
  };

  struct RowEvent {
    Json key;
    Json before;
    Json after;
    std::string operation;
    QualifiedTable table;
    std::uint64_t total_order = 0;
    std::uint64_t data_collection_order = 0;
    std::string stream_id;
    std::size_t raw_bytes = 0;
  };

  struct ActiveTransaction {
    std::string structural_id;
    std::string begin_stream_id;
    std::vector<RowEvent> rows;
    std::unordered_map<std::string, std::uint64_t> table_counts;
    std::size_t raw_bytes = 0;
  };

  struct Failure {
    CdcErrorCode code;
    std::string message;
    std::string stream_id;
  };

  struct MarkerUpdate {
    RunScopedCorrelationId correlation_id;
    std::string session_id;
    std::uint64_t txn_id = 0;
    std::string identity;
  };

  struct YcsbImage {
    std::int32_t key = 0;
    std::array<std::optional<std::string>, 10> fields;

    bool operator==(const YcsbImage &) const = default;
  };

  [[noreturn]] void fail(const CdcErrorCode code,
                         const std::string &message) const {
    throw CdcAssemblerError(code, message, current_stream_id_);
  }

  void recordAcceptedRaw(const CdcStreamRecord &record) noexcept {
    const auto add_saturating = [](const std::uint64_t current,
                                   const std::uint64_t additional) {
      return additional > std::numeric_limits<std::uint64_t>::max() - current
                 ? std::numeric_limits<std::uint64_t>::max()
                 : current + additional;
    };
    accepted_raw_records_ = add_saturating(accepted_raw_records_, 1U);
    accepted_raw_bytes_ = add_saturating(
        accepted_raw_bytes_,
        static_cast<std::uint64_t>(record.key_json.size()) +
            static_cast<std::uint64_t>(record.envelope_json.size()));
  }

  void rememberFailure(const CdcAssemblerError &error) {
    if (!failure_.has_value()) {
      failure_ = Failure{.code = error.code(),
                         .message = error.what(),
                         .stream_id = error.streamId()};
    }
  }

  void validateConfig() {
    if (!isLowerUuid(config_.run_id)) {
      throw std::invalid_argument("assembler run_id is not a lowercase UUID");
    }
    if (config_.backend_source_id.empty() ||
        config_.backend_source_id.size() > 256U) {
      throw std::invalid_argument("assembler backend_source_id is invalid");
    }
    if (config_.debezium_version.empty()) {
      throw std::invalid_argument("assembler Debezium version is empty");
    }
    if (config_.application_tables.empty()) {
      throw std::invalid_argument(
          "assembler application table allowlist is empty");
    }
    if (config_.max_active_transactions == 0U ||
        config_.max_events_per_transaction == 0U ||
        config_.max_active_bytes == 0U ||
        config_.max_completed_transactions == 0U ||
        config_.max_retained_identity_bytes == 0U) {
      throw std::invalid_argument("assembler resource limits must be positive");
    }

    std::unordered_set<std::string> all_tables;
    for (const auto &name : config_.application_tables) {
      const auto parsed = parseQualifiedTable(name);
      if (!parsed.has_value() || !all_tables.insert(name).second) {
        throw std::invalid_argument(
            "assembler application table is invalid or duplicated: " + name);
      }
      application_tables_.emplace(name, *parsed);
    }

    const auto marker = parseQualifiedTable(config_.transaction_marker_table);
    const auto fence = parseQualifiedTable(config_.run_fence_table);
    if (!marker.has_value() || !fence.has_value()) {
      throw std::invalid_argument("assembler control table name is invalid");
    }
    if (!all_tables.insert(marker->full_name).second ||
        !all_tables.insert(fence->full_name).second) {
      throw std::invalid_argument("assembler table roles overlap");
    }
    marker_table_ = *marker;
    fence_table_ = *fence;
  }

  void validateStreamOrder(const CdcStreamRecord &record) {
    const auto parsed = parseStreamId(record.stream_id);
    if (!parsed.has_value()) {
      fail(CdcErrorCode::CdcOrderInvalid,
           "CDC record has a non-canonical Redis stream ID");
    }
    if (parsed->milliseconds == 0U && parsed->sequence == 0U) {
      fail(CdcErrorCode::CdcOrderInvalid,
           "0-0 is a Redis cursor sentinel, not a CDC entry ID");
    }
    if (last_stream_id_.has_value()) {
      if (*parsed == *last_stream_id_) {
        fail(CdcErrorCode::CdcDuplicateConflict,
             "CDC Redis stream ID was delivered more than once");
      }
      if (*parsed < *last_stream_id_) {
        fail(CdcErrorCode::CdcOrderInvalid, "CDC Redis stream IDs regressed");
      }
    }
    last_stream_id_ = *parsed;
  }

  void requireObject(
      const Json &value, const std::string_view label,
      const CdcErrorCode code = CdcErrorCode::CdcRecordMalformed) const {
    if (!value.is_object()) {
      fail(code, std::string(label) + " is not an object");
    }
  }

  void requireExactKeys(
      const Json &object,
      const std::initializer_list<std::string_view> expected,
      const std::string_view label,
      const CdcErrorCode code = CdcErrorCode::CdcRecordMalformed) const {
    requireObject(object, label, code);
    std::set<std::string, std::less<>> remaining;
    for (const auto key : expected) {
      remaining.emplace(key);
    }
    for (const auto &[key, unused] : object.items()) {
      (void)unused;
      if (remaining.erase(key) == 0U) {
        fail(code, std::string(label) + " contains unexpected field " + key);
      }
    }
    if (!remaining.empty()) {
      fail(code,
           std::string(label) + " is missing field " + *remaining.begin());
    }
  }

  [[nodiscard]] const Json &
  member(const Json &object, const std::string &name,
         const std::string_view label,
         const CdcErrorCode code = CdcErrorCode::CdcRecordMalformed) const {
    requireObject(object, label, code);
    const auto found = object.find(name);
    if (found == object.end()) {
      fail(code, std::string(label) + " is missing field " + name);
    }
    return *found;
  }

  [[nodiscard]] std::string stringValue(
      const Json &value, const std::string_view label,
      const CdcErrorCode code = CdcErrorCode::CdcRecordMalformed) const {
    if (!value.is_string()) {
      fail(code, std::string(label) + " is not a string");
    }
    return value.get<std::string>();
  }

  [[nodiscard]] std::uint64_t uintValue(
      const Json &value, const std::string_view label,
      const std::uint64_t maximum = std::numeric_limits<std::uint64_t>::max(),
      const CdcErrorCode code = CdcErrorCode::CdcRecordMalformed) const {
    std::uint64_t result = 0;
    if (value.is_number_unsigned()) {
      result = value.get<std::uint64_t>();
    } else if (value.is_number_integer()) {
      const auto signed_value = value.get<std::int64_t>();
      if (signed_value < 0) {
        fail(code, std::string(label) + " is negative");
      }
      result = static_cast<std::uint64_t>(signed_value);
    } else {
      fail(code, std::string(label) + " is not an integer");
    }
    if (result > maximum) {
      fail(code, std::string(label) + " exceeds its supported range");
    }
    return result;
  }

  void validateNullableString(const Json &value,
                              const std::string_view label) const {
    if (!value.is_null() && !value.is_string()) {
      fail(CdcErrorCode::DdlObserved,
           std::string(label) + " is neither null nor a string");
    }
  }

  void validateSnapshotSchemaColumn(const Json &column) const {
    constexpr auto code = CdcErrorCode::DdlObserved;
    requireExactKeys(column,
                     {"name", "jdbcType", "nativeType", "typeName",
                      "typeExpression", "charsetName", "length", "scale",
                      "position", "optional", "autoIncremented", "generated",
                      "comment", "defaultValueExpression", "enumValues"},
                     "startup schema snapshot column", code);
    if (stringValue(member(column, "name", "startup schema column", code),
                    "startup schema column.name", code)
            .empty() ||
        !member(column, "jdbcType", "startup schema column", code)
             .is_number_integer()) {
      fail(code, "startup schema snapshot column identity is malformed");
    }
    validateNullableString(
        member(column, "nativeType", "startup schema column", code),
        "startup schema column.nativeType");
    (void)stringValue(member(column, "typeName", "startup schema column", code),
                      "startup schema column.typeName", code);
    (void)stringValue(
        member(column, "typeExpression", "startup schema column", code),
        "startup schema column.typeExpression", code);
    validateNullableString(
        member(column, "charsetName", "startup schema column", code),
        "startup schema column.charsetName");
    for (const auto field : {"length", "scale"}) {
      const auto &value = member(column, field, "startup schema column", code);
      if (!value.is_null() && !value.is_number_integer()) {
        fail(code, "startup schema column numeric metadata is malformed");
      }
    }
    if (uintValue(member(column, "position", "startup schema column", code),
                  "startup schema column.position",
                  std::numeric_limits<std::uint32_t>::max(), code) == 0U) {
      fail(code, "startup schema column.position must be positive");
    }
    for (const auto field : {"optional", "autoIncremented", "generated"}) {
      if (!member(column, field, "startup schema column", code).is_boolean()) {
        fail(code, "startup schema column boolean metadata is malformed");
      }
    }
    validateNullableString(
        member(column, "comment", "startup schema column", code),
        "startup schema column.comment");
    validateNullableString(
        member(column, "defaultValueExpression", "startup schema column", code),
        "startup schema column.defaultValueExpression");
    const auto &enum_values =
        member(column, "enumValues", "startup schema column", code);
    if (!enum_values.is_null()) {
      if (!enum_values.is_array()) {
        fail(code, "startup schema column.enumValues is malformed");
      }
      for (const auto &value : enum_values) {
        (void)stringValue(value, "startup schema column enum value", code);
      }
    }
  }

  void validateSnapshotTableDefinition(const Json &table) const {
    constexpr auto code = CdcErrorCode::DdlObserved;
    requireExactKeys(
        table,
        {"defaultCharsetName", "primaryKeyColumnNames", "columns", "comment"},
        "startup schema snapshot table", code);
    validateNullableString(
        member(table, "defaultCharsetName", "startup schema table", code),
        "startup schema table.defaultCharsetName");
    const auto &primary_keys =
        member(table, "primaryKeyColumnNames", "startup schema table", code);
    const auto &columns =
        member(table, "columns", "startup schema table", code);
    if (!primary_keys.is_array() || !columns.is_array() || columns.empty()) {
      fail(code, "startup schema snapshot table arrays are malformed");
    }
    for (const auto &primary_key : primary_keys) {
      (void)stringValue(primary_key, "startup schema primary-key name", code);
    }
    for (const auto &column : columns) {
      validateSnapshotSchemaColumn(column);
    }
    validateNullableString(
        member(table, "comment", "startup schema table", code),
        "startup schema table.comment");
  }

  [[nodiscard]] static std::string
  snapshotTableIdentifier(const QualifiedTable &table) {
    return "\"" + table.schema + "\".\"" + table.table + "\"";
  }

  [[nodiscard]] bool
  snapshotContainsConfiguredTable(const QualifiedTable &table) const {
    return std::binary_search(startup_schema_dropped_table_ids_.begin(),
                              startup_schema_dropped_table_ids_.end(),
                              snapshotTableIdentifier(table));
  }

  void requireConfiguredSnapshotTables() const {
    constexpr auto code = CdcErrorCode::DdlObserved;
    for (const auto &[unused, table] : application_tables_) {
      (void)unused;
      if (!snapshotContainsConfiguredTable(table)) {
        fail(code, "startup schema snapshot is missing a configured table");
      }
    }
    if (!snapshotContainsConfiguredTable(marker_table_) ||
        !snapshotContainsConfiguredTable(fence_table_)) {
      fail(code, "startup schema snapshot is missing a configured table");
    }
  }

  void requireSnapshotFlag(const std::string_view actual,
                           const std::string_view expected) const {
    if (actual != expected) {
      fail(CdcErrorCode::DdlObserved,
           "startup schema snapshot marker is out of order");
    }
  }

  void
  acceptStartupSchemaSemantics(const std::string &database,
                               const std::optional<std::string> &source_table,
                               const std::string &ddl, const Json &changes,
                               const std::string &snapshot) {
    constexpr auto code = CdcErrorCode::DdlObserved;
    if (!changes.is_array()) {
      fail(code, "startup schema snapshot tableChanges is not an array");
    }

    if (changes.empty()) {
      if (source_table.has_value()) {
        fail(code, "startup database schema record unexpectedly names a table");
      }
      switch (startup_schema_stage_) {
      case StartupSchemaStage::ServerSettings:
        if (!database.empty() || !std::string_view(ddl).starts_with("SET ")) {
          fail(code, "startup server-settings schema record is malformed");
        }
        requireSnapshotFlag(snapshot, "true");
        startup_schema_stage_ = StartupSchemaStage::DropTables;
        return;
      case StartupSchemaStage::DropTables:
        if (database != marker_table_.schema ||
            !std::string_view(ddl).starts_with("DROP DATABASE ") ||
            startup_schema_dropped_table_ids_.empty()) {
          fail(code, "startup DROP DATABASE record is out of order");
        }
        requireSnapshotFlag(snapshot, "true");
        requireConfiguredSnapshotTables();
        startup_schema_stage_ = StartupSchemaStage::CreateDatabase;
        return;
      case StartupSchemaStage::CreateDatabase:
        if (database != marker_table_.schema ||
            !std::string_view(ddl).starts_with("CREATE DATABASE ")) {
          fail(code, "startup CREATE DATABASE record is out of order");
        }
        requireSnapshotFlag(snapshot, "true");
        startup_schema_stage_ = StartupSchemaStage::UseDatabase;
        return;
      case StartupSchemaStage::UseDatabase:
        if (database != marker_table_.schema ||
            !std::string_view(ddl).starts_with("USE ")) {
          fail(code, "startup USE DATABASE record is out of order");
        }
        requireSnapshotFlag(snapshot, "true");
        startup_schema_stage_ = StartupSchemaStage::CreateTables;
        return;
      case StartupSchemaStage::CreateTables:
      case StartupSchemaStage::Complete:
        fail(code, "empty startup schema record is out of order");
      }
    }

    if (changes.size() != 1U || database != marker_table_.schema ||
        !source_table.has_value()) {
      fail(code, "startup table schema record is malformed");
    }
    const auto &change = changes.front();
    requireExactKeys(change, {"type", "id", "table"},
                     "startup schema snapshot table change", code);
    const auto type =
        stringValue(member(change, "type", "startup schema table change", code),
                    "startup schema table change.type", code);
    const auto identifier =
        stringValue(member(change, "id", "startup schema table change", code),
                    "startup schema table change.id", code);
    const QualifiedTable source_qualified{
        .schema = database,
        .table = *source_table,
        .full_name = database + "." + *source_table,
    };
    if (identifier != snapshotTableIdentifier(source_qualified)) {
      fail(code, "startup schema table-change/source identity mismatch");
    }
    const auto &table =
        member(change, "table", "startup schema table change", code);

    if (startup_schema_stage_ == StartupSchemaStage::DropTables) {
      if (type != "DROP" || !table.is_null() ||
          !std::string_view(ddl).starts_with("DROP TABLE ")) {
        fail(code, "startup DROP TABLE record is malformed");
      }
      requireSnapshotFlag(snapshot, "true");
      if (!startup_schema_dropped_table_ids_.empty() &&
          identifier <= startup_schema_dropped_table_ids_.back()) {
        fail(code,
             "startup DROP TABLE identifiers are duplicated or reordered");
      }
      startup_schema_dropped_table_ids_.push_back(identifier);
      return;
    }

    if (startup_schema_stage_ != StartupSchemaStage::CreateTables ||
        type != "CREATE" ||
        !std::string_view(ddl).starts_with("CREATE TABLE ")) {
      fail(code, "startup CREATE TABLE record is out of order");
    }
    validateSnapshotTableDefinition(table);
    if (startup_schema_create_index_ >=
            startup_schema_dropped_table_ids_.size() ||
        identifier !=
            startup_schema_dropped_table_ids_[startup_schema_create_index_]) {
      fail(code, "startup CREATE TABLE identifiers do not match ordered drops");
    }
    const bool final_table = startup_schema_create_index_ + 1U ==
                             startup_schema_dropped_table_ids_.size();
    requireSnapshotFlag(snapshot, final_table ? "last" : "true");
    ++startup_schema_create_index_;
    if (final_table) {
      startup_schema_stage_ = StartupSchemaStage::Complete;
      startup_schema_snapshot_finished_ = true;
    }
  }

  void acceptStartupSchemaSnapshot(const Json &key, const Json &envelope) {
    constexpr auto code = CdcErrorCode::DdlObserved;
    if (lifecycle_phase_ != LifecyclePhase::AwaitRunStart || !active_.empty() ||
        next_cdc_seq_ != 0U || startup_schema_snapshot_finished_) {
      fail(code, "Debezium schema-change/DDL envelope was observed after "
                 "transactional run evidence began");
    }
    if (startup_schema_snapshot_records_ >=
        config_.max_events_per_transaction) {
      fail(CdcErrorCode::ResourceExhausted,
           "startup schema snapshot record limit exceeded");
    }

    requireExactKeys(key, {"databaseName"}, "startup schema snapshot key",
                     code);
    requireExactKeys(envelope,
                     {"source", "ts_ms", "databaseName", "schemaName", "ddl",
                      "tableChanges"},
                     "startup schema snapshot envelope", code);
    const auto key_database = stringValue(
        member(key, "databaseName", "startup schema snapshot key", code),
        "startup schema snapshot key.databaseName", code);
    const auto database = stringValue(
        member(envelope, "databaseName", "startup schema snapshot", code),
        "startup schema snapshot.databaseName", code);
    if (key_database != database ||
        (!database.empty() && database != marker_table_.schema)) {
      fail(code, "startup schema snapshot database identity is invalid");
    }
    if (!member(envelope, "schemaName", "startup schema snapshot", code)
             .is_null()) {
      fail(code, "startup schema snapshot schemaName is unexpectedly non-null");
    }
    const auto ddl =
        stringValue(member(envelope, "ddl", "startup schema snapshot", code),
                    "startup schema snapshot.ddl", code);
    if (ddl.empty()) {
      fail(code, "startup schema snapshot DDL is empty");
    }
    (void)uintValue(member(envelope, "ts_ms", "startup schema snapshot", code),
                    "startup schema snapshot.ts_ms",
                    std::numeric_limits<std::uint64_t>::max(), code);
    const auto &table_changes =
        member(envelope, "tableChanges", "startup schema snapshot", code);

    const auto &source =
        member(envelope, "source", "startup schema snapshot", code);
    requireExactKeys(source,
                     {"version", "connector", "name", "ts_ms", "snapshot", "db",
                      "sequence", "ts_us", "ts_ns", "table", "server_id",
                      "gtid", "file", "pos", "row", "thread", "query"},
                     "startup schema snapshot source", code);
    if (stringValue(member(source, "version", "startup schema source", code),
                    "startup schema source.version",
                    code) != config_.debezium_version ||
        stringValue(member(source, "connector", "startup schema source", code),
                    "startup schema source.connector", code) != "mariadb" ||
        stringValue(member(source, "name", "startup schema source", code),
                    "startup schema source.name",
                    code) != config_.backend_source_id ||
        stringValue(member(source, "db", "startup schema source", code),
                    "startup schema source.db", code) != database) {
      fail(code, "startup schema snapshot source identity is invalid");
    }
    const auto snapshot =
        stringValue(member(source, "snapshot", "startup schema source", code),
                    "startup schema source.snapshot", code);
    if (snapshot != "true" && snapshot != "last") {
      fail(code, "non-snapshot DDL was observed");
    }
    for (const auto field : {"sequence", "gtid", "thread", "query"}) {
      if (!member(source, field, "startup schema source", code).is_null()) {
        fail(code, "startup schema source nullable metadata is non-null");
      }
    }
    const auto &table_value =
        member(source, "table", "startup schema source", code);
    std::optional<std::string> source_table;
    if (!table_value.is_null()) {
      const auto table_name =
          stringValue(table_value, "startup schema source.table", code);
      if (database.empty() || !isCanonicalIdentifier(table_name)) {
        fail(code, "startup schema source table identity is invalid");
      }
      source_table = table_name;
    }
    if (uintValue(member(source, "server_id", "startup schema source", code),
                  "startup schema source.server_id",
                  std::numeric_limits<std::uint64_t>::max(), code) != 0U) {
      fail(code, "startup schema snapshot server_id is not zero");
    }
    const auto file =
        stringValue(member(source, "file", "startup schema source", code),
                    "startup schema source.file", code);
    if (file.empty()) {
      fail(code, "startup schema snapshot source.file is empty");
    }
    for (const auto field : {"ts_ms", "ts_us", "ts_ns", "pos", "row"}) {
      (void)uintValue(member(source, field, "startup schema source", code),
                      std::string("startup schema source.") + field,
                      std::numeric_limits<std::uint64_t>::max(), code);
    }

    acceptStartupSchemaSemantics(database, source_table, ddl, table_changes,
                                 snapshot);
    ++startup_schema_snapshot_records_;
    saw_startup_schema_snapshot_ = true;
  }

  [[nodiscard]] std::string validateMetadataKey(const Json &key) const {
    requireExactKeys(key, {"id"}, "Debezium transaction metadata key");
    const auto identifier =
        stringValue(member(key, "id", "Debezium transaction metadata key"),
                    "Debezium transaction metadata key.id");
    if (identifier.empty() || identifier.size() > 512U) {
      fail(CdcErrorCode::CdcRecordMalformed,
           "Debezium transaction metadata key.id is empty or oversized");
    }
    return identifier;
  }

  [[nodiscard]] std::optional<CdcTransaction>
  acceptTransactionMetadata(const CdcStreamRecord &record, const Json &key,
                            const Json &envelope) {
    requireExactKeys(
        envelope, {"status", "id", "event_count", "data_collections", "ts_ms"},
        "Debezium transaction metadata");
    const auto key_id = validateMetadataKey(key);
    const auto status =
        stringValue(member(envelope, "status", "transaction metadata"),
                    "transaction metadata.status");
    if (!startup_schema_snapshot_finished_) {
      if (!saw_startup_schema_snapshot_) {
        fail(CdcErrorCode::CdcRecordMalformed,
             "transactional evidence began without the required startup "
             "schema snapshot");
      }
      fail(CdcErrorCode::DdlObserved,
           "transactional evidence began before the startup schema snapshot "
           "reached its last record");
    }
    const auto envelope_id =
        stringValue(member(envelope, "id", "transaction metadata"),
                    "transaction metadata.id");
    if (envelope_id.empty() || envelope_id != key_id) {
      fail(CdcErrorCode::CdcTransactionIdMismatch,
           "Debezium transaction metadata key and envelope IDs disagree");
    }
    (void)uintValue(member(envelope, "ts_ms", "transaction metadata"),
                    "transaction metadata.ts_ms");

    if (status == "BEGIN") {
      if (!member(envelope, "event_count", "BEGIN").is_null() ||
          !member(envelope, "data_collections", "BEGIN").is_null()) {
        fail(CdcErrorCode::CdcRecordMalformed,
             "Debezium BEGIN must have null event_count and data_collections");
      }
      if (active_.contains(envelope_id) ||
          completed_structural_ids_.contains(envelope_id)) {
        fail(CdcErrorCode::CdcDuplicateConflict,
             "Debezium transaction structural ID was reused");
      }
      if (!active_.empty()) {
        fail(CdcErrorCode::CdcOrderInvalid,
             "nested or interleaved Debezium BEGIN violates contiguous "
             "MariaDB transaction framing");
      }
      if (active_.size() >= config_.max_active_transactions) {
        fail(CdcErrorCode::ResourceExhausted,
             "active Debezium transaction limit exceeded");
      }
      const auto raw_bytes =
          record.key_json.size() + record.envelope_json.size();
      reserveActiveBytes(raw_bytes);
      ActiveTransaction transaction;
      transaction.structural_id = envelope_id;
      transaction.begin_stream_id = record.stream_id;
      transaction.raw_bytes = raw_bytes;
      active_.emplace(envelope_id, std::move(transaction));
      ++begin_metadata_records_;
      return std::nullopt;
    }

    if (status != "END") {
      fail(CdcErrorCode::CdcRecordMalformed,
           "unknown Debezium transaction status " + status);
    }
    if (completed_structural_ids_.contains(envelope_id)) {
      fail(CdcErrorCode::CdcDuplicateConflict,
           "Debezium END was delivered for an already completed transaction");
    }
    auto active = active_.find(envelope_id);
    if (active == active_.end()) {
      fail(CdcErrorCode::CdcBeginMissing,
           "Debezium END has no matching active BEGIN");
    }
    auto completed = completeTransaction(active, envelope, record.stream_id);
    ++end_metadata_records_;
    return completed;
  }

  void validateSource(const Json &source, const std::string &transaction_id,
                      QualifiedTable &table) const {
    requireExactKeys(source,
                     {"version", "connector", "name", "ts_ms", "snapshot", "db",
                      "sequence", "ts_us", "ts_ns", "table", "server_id",
                      "gtid", "file", "pos", "row", "thread", "query"},
                     "Debezium MariaDB source metadata");
    const auto version =
        stringValue(member(source, "version", "source"), "source.version");
    const auto connector =
        stringValue(member(source, "connector", "source"), "source.connector");
    const auto source_name =
        stringValue(member(source, "name", "source"), "source.name");
    if (version != config_.debezium_version || connector != "mariadb") {
      fail(CdcErrorCode::CdcRecordMalformed,
           "unsupported Debezium connector kind or version");
    }
    if (source_name != config_.backend_source_id) {
      fail(CdcErrorCode::CdcRecordMalformed,
           "CDC source identity differs from the configured backend source");
    }

    const auto database =
        stringValue(member(source, "db", "source"), "source.db");
    const auto table_name =
        stringValue(member(source, "table", "source"), "source.table");
    if (!isCanonicalIdentifier(database) ||
        !isCanonicalIdentifier(table_name)) {
      fail(CdcErrorCode::CdcRecordMalformed,
           "CDC source table identifiers are not canonical lowercase names");
    }
    table = QualifiedTable{.schema = database,
                           .table = table_name,
                           .full_name = database + "." + table_name};

    const auto source_transaction_id =
        stringValue(member(source, "gtid", "source"), "source.gtid");
    if (source_transaction_id != transaction_id) {
      fail(CdcErrorCode::CdcTransactionIdMismatch,
           "row source GTID disagrees with its Debezium transaction ID");
    }

    const auto &snapshot = member(source, "snapshot", "source");
    const bool snapshot_false =
        (snapshot.is_boolean() && !snapshot.get<bool>()) ||
        (snapshot.is_string() && snapshot.get<std::string>() == "false");
    if (!snapshot_false) {
      fail(CdcErrorCode::ExternalWriterObserved,
           "snapshot CDC row is outside the captured live run");
    }
    if (!member(source, "sequence", "source").is_null()) {
      fail(CdcErrorCode::CdcRecordMalformed,
           "MariaDB source.sequence is unexpectedly non-null");
    }
    if (!member(source, "query", "source").is_null()) {
      fail(CdcErrorCode::CdcRecordMalformed,
           "MariaDB source.query is unexpectedly non-null");
    }
    const auto &thread = member(source, "thread", "source");
    if (!thread.is_null()) {
      (void)uintValue(thread, "source.thread");
    }
    const auto file =
        stringValue(member(source, "file", "source"), "source.file");
    if (file.empty()) {
      fail(CdcErrorCode::CdcRecordMalformed, "source.file is empty");
    }
    (void)uintValue(member(source, "ts_ms", "source"), "source.ts_ms");
    (void)uintValue(member(source, "ts_us", "source"), "source.ts_us");
    (void)uintValue(member(source, "ts_ns", "source"), "source.ts_ns");
    (void)uintValue(member(source, "server_id", "source"), "source.server_id");
    (void)uintValue(member(source, "pos", "source"), "source.pos");
    (void)uintValue(member(source, "row", "source"), "source.row");
  }

  void acceptRow(const CdcStreamRecord &record, const Json &key,
                 const Json &envelope) {
    requireExactKeys(envelope,
                     {"before", "after", "source", "transaction", "op", "ts_ms",
                      "ts_us", "ts_ns"},
                     "Debezium row envelope");
    requireObject(key, "Debezium row key");
    const auto operation =
        stringValue(member(envelope, "op", "row envelope"), "row envelope.op");
    if (operation != "c" && operation != "u" && operation != "d") {
      fail(CdcErrorCode::CdcRecordMalformed,
           "unsupported Debezium row operation " + operation);
    }
    const auto &transaction = member(envelope, "transaction", "row envelope");
    requireExactKeys(transaction,
                     {"id", "total_order", "data_collection_order"},
                     "Debezium row transaction metadata");
    const auto transaction_id = stringValue(
        member(transaction, "id", "row transaction"), "row transaction.id");
    if (transaction_id.empty()) {
      fail(CdcErrorCode::CdcRecordMalformed, "row transaction.id is empty");
    }
    if (completed_structural_ids_.contains(transaction_id)) {
      fail(CdcErrorCode::CdcDuplicateConflict,
           "row was delivered for an already completed transaction");
    }
    auto active = active_.find(transaction_id);
    if (active == active_.end()) {
      fail(CdcErrorCode::CdcBeginMissing,
           "row event has no matching active Debezium BEGIN");
    }

    QualifiedTable table;
    validateSource(member(envelope, "source", "row envelope"), transaction_id,
                   table);
    const bool application = application_tables_.contains(table.full_name);
    const bool marker = table.full_name == marker_table_.full_name;
    const bool fence = table.full_name == fence_table_.full_name;
    if (!application && !marker && !fence) {
      fail(CdcErrorCode::UnexpectedTable,
           "CDC row names a table outside the configured allowlist: " +
               table.full_name);
    }

    const auto total_order =
        uintValue(member(transaction, "total_order", "row transaction"),
                  "row transaction.total_order");
    const auto collection_order = uintValue(
        member(transaction, "data_collection_order", "row transaction"),
        "row transaction.data_collection_order");
    const auto expected_total =
        static_cast<std::uint64_t>(active->second.rows.size()) + 1U;
    const auto expected_collection =
        active->second.table_counts[table.full_name] + 1U;
    if (total_order != expected_total ||
        collection_order != expected_collection) {
      fail(CdcErrorCode::CdcEventOrdinalInvalid,
           "CDC row ordinals are repeated, regressed, or non-contiguous");
    }
    if (active->second.rows.size() >= config_.max_events_per_transaction) {
      fail(CdcErrorCode::ResourceExhausted,
           "CDC transaction event limit exceeded");
    }

    const auto &before = member(envelope, "before", "row envelope");
    const auto &after = member(envelope, "after", "row envelope");
    if (!(before.is_null() || before.is_object()) ||
        !(after.is_null() || after.is_object())) {
      fail(CdcErrorCode::CdcRecordMalformed,
           "row before/after images must be objects or null");
    }
    (void)uintValue(member(envelope, "ts_ms", "row envelope"),
                    "row envelope.ts_ms");
    (void)uintValue(member(envelope, "ts_us", "row envelope"),
                    "row envelope.ts_us");
    (void)uintValue(member(envelope, "ts_ns", "row envelope"),
                    "row envelope.ts_ns");

    const auto raw_bytes = record.key_json.size() + record.envelope_json.size();
    reserveActiveBytes(raw_bytes);
    active->second.rows.push_back(RowEvent{
        .key = key,
        .before = before,
        .after = after,
        .operation = operation,
        .table = std::move(table),
        .total_order = total_order,
        .data_collection_order = collection_order,
        .stream_id = record.stream_id,
        .raw_bytes = raw_bytes,
    });
    active->second.table_counts[active->second.rows.back().table.full_name] =
        collection_order;
    active->second.raw_bytes += raw_bytes;
    ++row_records_;
  }

  void reserveActiveBytes(const std::size_t additional) {
    if (additional > config_.max_active_bytes ||
        active_bytes_ > config_.max_active_bytes - additional) {
      fail(CdcErrorCode::ResourceExhausted,
           "active CDC assembly byte limit exceeded");
    }
    active_bytes_ += additional;
    active_bytes_high_water_ =
        std::max(active_bytes_high_water_, active_bytes_);
  }

  void reserveRetainedIdentityBytes(const std::size_t additional) {
    if (additional > config_.max_retained_identity_bytes ||
        retained_identity_bytes_ >
            config_.max_retained_identity_bytes - additional) {
      fail(CdcErrorCode::ResourceExhausted,
           "retained CDC identity byte limit exceeded");
    }
    retained_identity_bytes_ += additional;
  }

  [[nodiscard]] std::optional<CdcTransaction> completeTransaction(
      const std::unordered_map<std::string, ActiveTransaction>::iterator active,
      const Json &end, const std::string &end_stream_id) {
    const auto declared_total =
        uintValue(member(end, "event_count", "END"), "END.event_count");
    if (declared_total != active->second.rows.size()) {
      fail(CdcErrorCode::CdcEventCountMismatch,
           "END.event_count differs from observed row events");
    }
    const auto &collections = member(end, "data_collections", "END");
    if (!collections.is_array()) {
      fail(CdcErrorCode::CdcEventCountMismatch,
           "END.data_collections is not an array");
    }
    std::unordered_map<std::string, std::uint64_t> declared_counts;
    std::uint64_t declared_sum = 0;
    for (const auto &collection : collections) {
      requireExactKeys(collection, {"data_collection", "event_count"},
                       "END data collection");
      const auto name =
          stringValue(member(collection, "data_collection", "END collection"),
                      "END data_collection");
      const auto count =
          uintValue(member(collection, "event_count", "END collection"),
                    "END data_collection.event_count");
      if (count == 0U || !declared_counts.emplace(name, count).second) {
        fail(CdcErrorCode::CdcEventCountMismatch,
             "END contains an empty or duplicate data-collection count");
      }
      if (declared_sum > std::numeric_limits<std::uint64_t>::max() - count) {
        fail(CdcErrorCode::CdcEventCountMismatch,
             "END per-table counts overflow");
      }
      declared_sum += count;
    }
    if (declared_sum != declared_total ||
        declared_counts != active->second.table_counts) {
      fail(CdcErrorCode::CdcEventCountMismatch,
           "END per-table counts differ from observed row events");
    }

    auto assembled = classifyAndNormalize(active->second, end_stream_id);
    if (next_cdc_seq_ >= config_.max_completed_transactions) {
      fail(CdcErrorCode::ResourceExhausted,
           "completed CDC transaction limit exceeded");
    }
    const auto structural_id = active->second.structural_id;
    const auto released_bytes = active->second.raw_bytes;
    const auto raw_row_count = active->second.rows.size();
    const auto normalized_write_count = assembled.writes.size();
    const auto classification = assembled.kind;
    reserveRetainedIdentityBytes(structural_id.size());
    active_.erase(active);
    active_bytes_ -= released_bytes;
    completed_structural_ids_.insert(structural_id);
    assembled.cdc_seq = next_cdc_seq_;
    ++next_cdc_seq_;
    raw_row_events_ += static_cast<std::uint64_t>(raw_row_count);
    normalized_write_events_ +=
        static_cast<std::uint64_t>(normalized_write_count);
    switch (classification) {
    case CdcTransactionKind::RunControl:
      ++completed_run_control_transactions_;
      break;
    case CdcTransactionKind::BootstrapApp:
      ++completed_bootstrap_transactions_;
      break;
    case CdcTransactionKind::WorkloadMarked:
      ++completed_workload_transactions_;
      break;
    }
    return assembled;
  }

  [[nodiscard]] CdcTransaction
  classifyAndNormalize(const ActiveTransaction &transaction,
                       const std::string &end_stream_id) {
    std::size_t application_count = 0;
    std::size_t marker_count = 0;
    std::size_t fence_count = 0;
    std::size_t marker_index = 0;
    for (std::size_t index = 0; index < transaction.rows.size(); ++index) {
      const auto &table = transaction.rows[index].table.full_name;
      if (application_tables_.contains(table)) {
        ++application_count;
      } else if (table == marker_table_.full_name) {
        marker_index = index;
        ++marker_count;
      } else if (table == fence_table_.full_name) {
        ++fence_count;
      }
    }

    CdcTransaction result{
        .cdc_seq = 0,
        .kind = CdcTransactionKind::BootstrapApp,
        .fence_phase = std::nullopt,
        .correlation_id = std::nullopt,
        .writes = {},
        .begin_stream_id = transaction.begin_stream_id,
        .end_stream_id = end_stream_id,
    };

    if (fence_count != 0U) {
      if (transaction.rows.size() != 1U || fence_count != 1U) {
        fail(CdcErrorCode::FenceOrderInvalid,
             "fence mutation is mixed with another CDC row");
      }
      const auto fence_phase = validateFence(transaction.rows.front());
      advanceFence(fence_phase);
      result.kind = CdcTransactionKind::RunControl;
      result.fence_phase = fence_phase;
      return result;
    }

    if (transaction.rows.empty()) {
      fail(CdcErrorCode::CdcRecordMalformed,
           "completed Debezium transaction contains no row events");
    }
    if (lifecycle_phase_ == LifecyclePhase::AwaitRunStart) {
      fail(CdcErrorCode::FenceOrderInvalid,
           "application or marker evidence precedes RUN_START");
    }
    if (lifecycle_phase_ == LifecyclePhase::Closed) {
      fail(CdcErrorCode::CdcApplicationAfterEnd,
           "application or marker evidence follows END_FENCE");
    }
    if (lifecycle_phase_ == LifecyclePhase::Bootstrap) {
      if (marker_count != 0U) {
        fail(CdcErrorCode::ExternalWriterObserved,
             "marker-bearing transaction appeared during bootstrap");
      }
      if (application_count != transaction.rows.size()) {
        fail(CdcErrorCode::UnexpectedTable,
             "bootstrap transaction contains non-application evidence");
      }
      result.kind = CdcTransactionKind::BootstrapApp;
      result.writes = normalizeApplicationWrites(transaction.rows);
      return result;
    }

    if (marker_count == 0U) {
      fail(application_count != 0U
               ? CdcErrorCode::UnmarkedApplicationTransaction
               : CdcErrorCode::MarkerMissing,
           "post-load transaction has no unified session_txn marker");
    }
    if (marker_count > 1U) {
      fail(CdcErrorCode::MarkerMultiple,
           "post-load transaction contains multiple session_txn markers");
    }
    if (marker_index + 1U != transaction.rows.size()) {
      fail(CdcErrorCode::MarkerNotLast,
           "session_txn marker is not the final row event");
    }
    if (application_count + 1U != transaction.rows.size()) {
      fail(
          CdcErrorCode::UnexpectedTable,
          "workload transaction contains unsupported non-application evidence");
    }

    const auto marker = validateMarker(transaction.rows[marker_index]);
    std::vector<RowEvent> application_rows;
    application_rows.reserve(application_count);
    for (const auto &row : transaction.rows) {
      if (application_tables_.contains(row.table.full_name)) {
        application_rows.push_back(row);
      }
    }
    result.kind = CdcTransactionKind::WorkloadMarked;
    result.correlation_id = marker.correlation_id;
    result.writes = normalizeApplicationWrites(application_rows);

    if (seen_marker_identities_.contains(marker.identity)) {
      fail(CdcErrorCode::MarkerDuplicate, "unified marker identity was reused");
    }
    const auto new_session =
        !last_marker_by_session_.contains(marker.session_id);
    reserveRetainedIdentityBytes(marker.identity.size() +
                                 (new_session ? marker.session_id.size() : 0U));
    // reserveRetainedIdentityBytes is intentionally before these insertions:
    // an allocation failure is sticky and cannot be mistaken for a valid run.
    seen_marker_identities_.insert(marker.identity);
    last_marker_by_session_[marker.session_id] = marker.txn_id;
    return result;
  }

  [[nodiscard]] RunFencePhase validateFence(const RowEvent &row) const {
    if (row.operation != "c" || !row.before.is_null() ||
        !row.after.is_object()) {
      fail(CdcErrorCode::FenceOrderInvalid,
           "run fence must be one insert with a null before-image");
    }
    requireExactKeys(row.key, {"run_id", "phase"}, "run fence key",
                     CdcErrorCode::FenceOrderInvalid);
    requireExactKeys(row.after, {"run_id", "phase", "nonce"},
                     "run fence after-image", CdcErrorCode::FenceOrderInvalid);
    const auto key_run =
        stringValue(member(row.key, "run_id", "run fence key"),
                    "run fence key.run_id", CdcErrorCode::FenceOrderInvalid);
    const auto key_phase =
        stringValue(member(row.key, "phase", "run fence key"),
                    "run fence key.phase", CdcErrorCode::FenceOrderInvalid);
    const auto after_run =
        stringValue(member(row.after, "run_id", "run fence after-image"),
                    "run fence after.run_id", CdcErrorCode::FenceOrderInvalid);
    const auto after_phase =
        stringValue(member(row.after, "phase", "run fence after-image"),
                    "run fence after.phase", CdcErrorCode::FenceOrderInvalid);
    const auto nonce =
        stringValue(member(row.after, "nonce", "run fence after-image"),
                    "run fence nonce", CdcErrorCode::FenceOrderInvalid);
    if (key_run != after_run || key_phase != after_phase) {
      fail(CdcErrorCode::FenceOrderInvalid,
           "run fence key and after-image disagree");
    }
    if (after_run != config_.run_id) {
      fail(CdcErrorCode::RunIdMismatch,
           "run fence belongs to a different run epoch");
    }
    if (!isLowerUuid(nonce)) {
      fail(CdcErrorCode::FenceOrderInvalid,
           "run fence nonce is not a lowercase UUID");
    }
    if (after_phase == "RUN_START") {
      return RunFencePhase::RunStart;
    }
    if (after_phase == "LOAD_FINISHED") {
      return RunFencePhase::LoadFinished;
    }
    if (after_phase == "END_FENCE") {
      return RunFencePhase::EndFence;
    }
    fail(CdcErrorCode::FenceOrderInvalid,
         "run fence phase is unknown: " + after_phase);
  }

  void advanceFence(const RunFencePhase phase) {
    if (seen_fences_.contains(phase)) {
      fail(CdcErrorCode::FenceDuplicate,
           "run fence phase was delivered more than once");
    }
    const bool legal = (phase == RunFencePhase::RunStart &&
                        lifecycle_phase_ == LifecyclePhase::AwaitRunStart) ||
                       (phase == RunFencePhase::LoadFinished &&
                        lifecycle_phase_ == LifecyclePhase::Bootstrap) ||
                       (phase == RunFencePhase::EndFence &&
                        lifecycle_phase_ == LifecyclePhase::Workload);
    if (!legal) {
      fail(CdcErrorCode::FenceOrderInvalid,
           "run fence phases are out of database commit order");
    }
    seen_fences_.insert(phase);
    switch (phase) {
    case RunFencePhase::RunStart:
      lifecycle_phase_ = LifecyclePhase::Bootstrap;
      break;
    case RunFencePhase::LoadFinished:
      lifecycle_phase_ = LifecyclePhase::Workload;
      break;
    case RunFencePhase::EndFence:
      lifecycle_phase_ = LifecyclePhase::Closed;
      break;
    }
  }

  [[nodiscard]] MarkerUpdate validateMarker(const RowEvent &row) const {
    if (row.operation == "d" || !row.after.is_object()) {
      fail(CdcErrorCode::MarkerMalformed,
           "session_txn marker must have a complete after-image");
    }
    requireExactKeys(row.key, {"session_id"}, "session_txn key",
                     CdcErrorCode::MarkerMalformed);
    requireExactKeys(row.after, {"session_id", "txn_id"},
                     "session_txn after-image", CdcErrorCode::MarkerMalformed);
    const auto key_session = stringValue(
        member(row.key, "session_id", "session_txn key",
               CdcErrorCode::MarkerMalformed),
        "session_txn key.session_id", CdcErrorCode::MarkerMalformed);
    const auto after_session =
        stringValue(member(row.after, "session_id", "marker after-image",
                           CdcErrorCode::MarkerMalformed),
                    "marker after.session_id", CdcErrorCode::MarkerMalformed);
    const auto after_txn =
        uintValue(member(row.after, "txn_id", "marker after-image",
                         CdcErrorCode::MarkerMalformed),
                  "marker after.txn_id", kMaxJsonSafeInteger,
                  CdcErrorCode::MarkerMalformed);
    if (!isSessionId(after_session) || key_session != after_session ||
        after_txn == 0U) {
      fail(CdcErrorCode::MarkerMalformed,
           "session_txn key/session/txn_id is malformed or inconsistent");
    }

    const std::string identity =
        after_session + '\0' + std::to_string(after_txn);
    if (seen_marker_identities_.contains(identity)) {
      fail(CdcErrorCode::MarkerDuplicate, "unified marker identity was reused");
    }
    const auto previous = last_marker_by_session_.find(after_session);
    if (previous == last_marker_by_session_.end()) {
      if (row.operation != "c" || !row.before.is_null()) {
        fail(CdcErrorCode::MarkerChainMismatch,
             "first committed session marker is not an insert with null "
             "before-image");
      }
    } else {
      if (row.operation != "u") {
        fail(CdcErrorCode::MarkerChainMismatch,
             "later committed session marker is not an update");
      }
      if (!row.before.is_object()) {
        fail(CdcErrorCode::MarkerBeforeImageMissing,
             "marker update lacks its complete before-image");
      }
      requireExactKeys(row.before, {"session_id", "txn_id"},
                       "session_txn before-image",
                       CdcErrorCode::MarkerBeforeImageMissing);
      const auto before_session = stringValue(
          member(row.before, "session_id", "marker before-image",
                 CdcErrorCode::MarkerBeforeImageMissing),
          "marker before.session_id", CdcErrorCode::MarkerBeforeImageMissing);
      const auto before_txn =
          uintValue(member(row.before, "txn_id", "marker before-image",
                           CdcErrorCode::MarkerBeforeImageMissing),
                    "marker before.txn_id", kMaxJsonSafeInteger,
                    CdcErrorCode::MarkerBeforeImageMissing);
      if (before_session != after_session || before_txn != previous->second ||
          after_txn <= previous->second) {
        fail(after_txn == previous->second ? CdcErrorCode::MarkerDuplicate
                                           : CdcErrorCode::MarkerChainMismatch,
             "marker before-image chain or monotonic txn_id is inconsistent");
      }
    }

    return MarkerUpdate{
        .correlation_id = RunScopedCorrelationId{.run_id = config_.run_id,
                                                 .session_id = after_session,
                                                 .txn_id = after_txn},
        .session_id = after_session,
        .txn_id = after_txn,
        .identity = identity,
    };
  }

  [[nodiscard]] YcsbImage parseYcsbImage(const Json &image,
                                         const std::string_view label) const {
    if (!image.is_object()) {
      fail(CdcErrorCode::CdcRecordMalformed,
           std::string(label) + " is not a complete row object");
    }
    if (!image.contains("ycsb_key")) {
      fail(CdcErrorCode::PrimaryKeyImageIncomplete,
           std::string(label) + " lacks ycsb_key");
    }
    std::set<std::string, std::less<>> expected{"ycsb_key"};
    for (std::size_t index = 1; index <= 10U; ++index) {
      expected.insert("field" + std::to_string(index));
    }
    for (const auto &[key, unused] : image.items()) {
      (void)unused;
      if (expected.erase(key) == 0U) {
        fail(CdcErrorCode::CdcRecordMalformed,
             std::string(label) + " contains unexpected field " + key);
      }
    }
    if (!expected.empty()) {
      fail(CdcErrorCode::CdcRecordMalformed,
           std::string(label) + " lacks field " + *expected.begin());
    }

    const auto raw_key =
        uintOrSignedKey(member(image, "ycsb_key", label), label);
    YcsbImage result;
    result.key = raw_key;
    for (std::size_t index = 0; index < result.fields.size(); ++index) {
      const auto field_name = "field" + std::to_string(index + 1U);
      const auto &field = member(image, field_name, label);
      if (field.is_null()) {
        result.fields[index] = std::nullopt;
      } else if (field.is_string()) {
        result.fields[index] = field.get<std::string>();
      } else {
        fail(CdcErrorCode::CdcRecordMalformed,
             std::string(label) + "." + field_name +
                 " is neither string nor SQL NULL");
      }
    }
    return result;
  }

  [[nodiscard]] std::int32_t
  uintOrSignedKey(const Json &value, const std::string_view label) const {
    std::int64_t converted = 0;
    if (value.is_number_unsigned()) {
      const auto unsigned_value = value.get<std::uint64_t>();
      if (unsigned_value > static_cast<std::uint64_t>(
                               std::numeric_limits<std::int32_t>::max())) {
        fail(CdcErrorCode::PrimaryKeyImageIncomplete,
             std::string(label) + " ycsb_key exceeds signed 32-bit range");
      }
      converted = static_cast<std::int64_t>(unsigned_value);
    } else if (value.is_number_integer()) {
      converted = value.get<std::int64_t>();
    } else {
      fail(CdcErrorCode::PrimaryKeyImageIncomplete,
           std::string(label) + " ycsb_key is not an integer");
    }
    if (converted < std::numeric_limits<std::int32_t>::min() ||
        converted > std::numeric_limits<std::int32_t>::max()) {
      fail(CdcErrorCode::PrimaryKeyImageIncomplete,
           std::string(label) + " ycsb_key exceeds signed 32-bit range");
    }
    return static_cast<std::int32_t>(converted);
  }

  [[nodiscard]] std::int32_t
  parseYcsbKeyObject(const Json &key, const std::string_view label) const {
    requireExactKeys(key, {"ycsb_key"}, label,
                     CdcErrorCode::PrimaryKeyImageIncomplete);
    return uintOrSignedKey(
        member(key, "ycsb_key", label, CdcErrorCode::PrimaryKeyImageIncomplete),
        label);
  }

  [[nodiscard]] std::vector<NormalizedWrite>
  normalizeApplicationWrites(const std::vector<RowEvent> &rows) const {
    std::map<CanonicalBytes, NormalizedWrite, CanonicalBytesLess> collapsed;
    using LocalRowKey = std::pair<std::string, std::int32_t>;
    std::map<LocalRowKey, std::optional<YcsbImage>> local_state;
    const auto put_present = [&collapsed](CanonicalBytes key,
                                          CanonicalBytes value) {
      NormalizedWrite write{.key = key,
                            .state = NormalizedValueState::Present,
                            .value = std::move(value)};
      collapsed.insert_or_assign(std::move(key), std::move(write));
    };
    const auto put_absent = [&collapsed](CanonicalBytes key) {
      NormalizedWrite write{.key = key,
                            .state = NormalizedValueState::Absent,
                            .value = std::nullopt};
      collapsed.insert_or_assign(std::move(key), std::move(write));
    };
    const auto require_before_matches =
        [this, &local_state](const LocalRowKey &key, const YcsbImage &before,
                             const std::string_view operation) {
          const auto previous = local_state.find(key);
          if (previous == local_state.end()) {
            return;
          }
          if (!previous->second.has_value()) {
            fail(CdcErrorCode::CdcRecordMalformed,
                 "YCSB " + std::string(operation) +
                     " follows a transaction-local Absent row state");
          }
          if (*previous->second != before) {
            fail(CdcErrorCode::CdcRecordMalformed,
                 "YCSB " + std::string(operation) +
                     " before-image does not continue the prior after-image");
          }
        };
    const auto require_insert_follows_absent =
        [this, &local_state](const LocalRowKey &key) {
          const auto previous = local_state.find(key);
          if (previous != local_state.end() && previous->second.has_value()) {
            fail(CdcErrorCode::CdcRecordMalformed,
                 "YCSB insert follows a transaction-local Present row state");
          }
        };

    try {
      for (const auto &row : rows) {
        const auto key_object = parseYcsbKeyObject(row.key, "usertable key");
        if (row.operation == "c") {
          if (!row.before.is_null() || !row.after.is_object()) {
            fail(CdcErrorCode::CdcRecordMalformed,
                 "YCSB insert has invalid before/after images");
          }
          const auto after =
              parseYcsbImage(row.after, "YCSB insert after-image");
          if (key_object != after.key) {
            fail(CdcErrorCode::PrimaryKeyImageIncomplete,
                 "YCSB insert key and after-image disagree");
          }
          const LocalRowKey state_key{row.table.full_name, after.key};
          require_insert_follows_absent(state_key);
          local_state.insert_or_assign(state_key, after);
          put_present(CanonicalYcsbCodec::encodeKey(row.table.schema,
                                                    row.table.table, after.key),
                      CanonicalYcsbCodec::encodeValue(after.fields));
          continue;
        }
        if (row.operation == "d") {
          if (!row.before.is_object() || !row.after.is_null()) {
            fail(CdcErrorCode::CdcRecordMalformed,
                 "YCSB delete has invalid before/after images");
          }
          const auto before =
              parseYcsbImage(row.before, "YCSB delete before-image");
          if (key_object != before.key) {
            fail(CdcErrorCode::PrimaryKeyImageIncomplete,
                 "YCSB delete key and before-image disagree");
          }
          const LocalRowKey state_key{row.table.full_name, before.key};
          require_before_matches(state_key, before, "delete");
          local_state.insert_or_assign(state_key, std::nullopt);
          put_absent(CanonicalYcsbCodec::encodeKey(
              row.table.schema, row.table.table, before.key));
          continue;
        }

        if (!row.before.is_object() || !row.after.is_object()) {
          fail(CdcErrorCode::PrimaryKeyImageIncomplete,
               "YCSB update lacks complete before/after images");
        }
        const auto before =
            parseYcsbImage(row.before, "YCSB update before-image");
        const auto after = parseYcsbImage(row.after, "YCSB update after-image");
        if (key_object != after.key) {
          fail(CdcErrorCode::PrimaryKeyImageIncomplete,
               "YCSB update key and after-image disagree");
        }
        const LocalRowKey before_state_key{row.table.full_name, before.key};
        const LocalRowKey after_state_key{row.table.full_name, after.key};
        require_before_matches(before_state_key, before, "update");
        if (before.key != after.key) {
          require_insert_follows_absent(after_state_key);
          local_state.insert_or_assign(before_state_key, std::nullopt);
          put_absent(CanonicalYcsbCodec::encodeKey(
              row.table.schema, row.table.table, before.key));
        }
        local_state.insert_or_assign(after_state_key, after);
        put_present(CanonicalYcsbCodec::encodeKey(row.table.schema,
                                                  row.table.table, after.key),
                    CanonicalYcsbCodec::encodeValue(after.fields));
      }
    } catch (const CdcAssemblerError &) {
      throw;
    } catch (const CanonicalEncodingError &error) {
      fail(CdcErrorCode::CanonicalEncodingFailed,
           std::string("failed to canonically encode YCSB CDC row: ") +
               error.what());
    }

    std::vector<NormalizedWrite> result;
    result.reserve(collapsed.size());
    for (auto &[unused, write] : collapsed) {
      (void)unused;
      result.push_back(std::move(write));
    }
    return result;
  }

  DebeziumAssemblerConfig config_;
  std::unordered_map<std::string, QualifiedTable> application_tables_;
  QualifiedTable marker_table_;
  QualifiedTable fence_table_;
  std::unordered_map<std::string, ActiveTransaction> active_;
  std::unordered_set<std::string> completed_structural_ids_;
  std::unordered_map<std::string, std::uint64_t> last_marker_by_session_;
  std::unordered_set<std::string> seen_marker_identities_;
  std::vector<std::string> startup_schema_dropped_table_ids_;
  std::set<RunFencePhase> seen_fences_;
  LifecyclePhase lifecycle_phase_ = LifecyclePhase::AwaitRunStart;
  StartupSchemaStage startup_schema_stage_ = StartupSchemaStage::ServerSettings;
  std::optional<StreamId> last_stream_id_;
  std::uint64_t next_cdc_seq_ = 0;
  std::uint64_t accepted_raw_records_ = 0;
  std::uint64_t accepted_raw_bytes_ = 0;
  std::uint64_t begin_metadata_records_ = 0;
  std::uint64_t end_metadata_records_ = 0;
  std::uint64_t row_records_ = 0;
  std::uint64_t completed_run_control_transactions_ = 0;
  std::uint64_t completed_bootstrap_transactions_ = 0;
  std::uint64_t completed_workload_transactions_ = 0;
  std::uint64_t raw_row_events_ = 0;
  std::uint64_t normalized_write_events_ = 0;
  std::size_t active_bytes_ = 0;
  std::size_t active_bytes_high_water_ = 0;
  std::size_t retained_identity_bytes_ = 0;
  std::size_t startup_schema_snapshot_records_ = 0;
  std::size_t startup_schema_create_index_ = 0;
  std::optional<Failure> failure_;
  std::string current_stream_id_;
  bool saw_startup_schema_snapshot_ = false;
  bool startup_schema_snapshot_finished_ = false;
  bool finished_ = false;
};

DebeziumTxnAssembler::DebeziumTxnAssembler(DebeziumAssemblerConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

DebeziumTxnAssembler::~DebeziumTxnAssembler() = default;
DebeziumTxnAssembler::DebeziumTxnAssembler(DebeziumTxnAssembler &&) noexcept =
    default;
DebeziumTxnAssembler &
DebeziumTxnAssembler::operator=(DebeziumTxnAssembler &&) noexcept = default;

std::optional<CdcTransaction>
DebeziumTxnAssembler::accept(const CdcStreamRecord &record) {
  return impl_->accept(record);
}

void DebeziumTxnAssembler::finish() { impl_->finish(); }

bool DebeziumTxnAssembler::failed() const noexcept { return impl_->failed(); }

std::optional<CdcErrorCode> DebeziumTxnAssembler::errorCode() const noexcept {
  return impl_->errorCode();
}

std::uint64_t DebeziumTxnAssembler::completedCount() const noexcept {
  return impl_->completedCount();
}

std::size_t DebeziumTxnAssembler::activeTransactionCount() const noexcept {
  return impl_->activeTransactionCount();
}

std::size_t DebeziumTxnAssembler::activeBytes() const noexcept {
  return impl_->activeBytes();
}

std::size_t DebeziumTxnAssembler::retainedIdentityBytes() const noexcept {
  return impl_->retainedIdentityBytes();
}

DebeziumAssemblerMetrics DebeziumTxnAssembler::metrics() const noexcept {
  return impl_->metrics();
}

} // namespace blackbox
