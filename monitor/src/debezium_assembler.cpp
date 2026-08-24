#include "blackbox/debezium_assembler.h"

#include "strict_json.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
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

[[nodiscard]] std::uint64_t steadyClockNanoseconds() noexcept {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

[[nodiscard]] constexpr std::uint64_t
saturatingAdd(const std::uint64_t current,
              const std::uint64_t additional) noexcept {
  return additional > std::numeric_limits<std::uint64_t>::max() - current
             ? std::numeric_limits<std::uint64_t>::max()
             : current + additional;
}

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
    monotonic_clock_nanoseconds_ = config_.monotonic_clock_nanoseconds;
    if (!monotonic_clock_nanoseconds_) {
      monotonic_clock_nanoseconds_ = steadyClockNanoseconds;
    }
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
    const auto accept_started = monotonic_clock_nanoseconds_();
    current_timed_structural_id_.reset();
    completed_processing_before_current_ = 0U;
    try {
      if (record.key_json.size() > config_.max_active_bytes ||
          record.envelope_json.size() >
              config_.max_active_bytes - record.key_json.size()) {
        fail(CdcErrorCode::ResourceExhausted,
             "one CDC record exceeds the configured assembly byte limit");
      }
      validateStreamOrder(record);
      auto key = detail::parseStrictJson(record.key_json);
      auto envelope = detail::parseStrictJson(record.envelope_json);
      const auto decoded = cdc_decoder_->decode(key, envelope);
      std::optional<CdcTransaction> completed;
      switch (decoded.kind) {
      case BackendCdcRecordKind::StartupSchema:
        acceptStartupSchemaRecord(key, envelope);
        break;
      case BackendCdcRecordKind::Begin:
      case BackendCdcRecordKind::End:
        completed = acceptTransactionMetadata(record, decoded);
        break;
      case BackendCdcRecordKind::Row:
        acceptRow(record, decoded);
        break;
      }
      recordAcceptedRaw(record);
      recordAcceptProcessing(accept_started, completed);
      return completed;
    } catch (const CdcAssemblerError &error) {
      rememberFailure(error);
      throw;
    } catch (const BackendCdcDecodeError &error) {
      const CdcAssemblerError wrapped(error.code(), error.what(),
                                      record.stream_id);
      rememberFailure(wrapped);
      throw wrapped;
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
    if (!startup_schema_decoder_->sawRecord()) {
      const CdcAssemblerError error(
          CdcErrorCode::CdcRecordMalformed,
          "end of input reached without the required startup schema snapshot",
          current_stream_id_);
      rememberFailure(error);
      throw error;
    }
    if (!startup_schema_decoder_->complete()) {
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
        .workload_raw_row_events = workload_raw_row_events_,
        .normalized_write_events = normalized_write_events_,
        .workload_normalized_write_events = workload_normalized_write_events_,
        .accept_total_nanoseconds = accept_total_nanoseconds_,
        .accept_max_nanoseconds = accept_max_nanoseconds_,
        .workload_processing_total_nanoseconds =
            workload_processing_total_nanoseconds_,
        .workload_processing_max_nanoseconds =
            workload_processing_max_nanoseconds_,
        .active_transactions = active_.size(),
        .active_transactions_high_water = active_transactions_high_water_,
        .observed_events_per_transaction_high_water =
            observed_events_per_transaction_high_water_,
        .active_assembly_bytes = active_bytes_,
        .active_assembly_high_water_bytes = active_bytes_high_water_,
        .retained_identity_records = completed_structural_ids_.size(),
        .retained_identity_bytes = retained_identity_bytes_,
        .retained_identity_high_water_records =
            retained_identity_high_water_records_,
        .retained_identity_high_water_bytes =
            retained_identity_high_water_bytes_,
    };
  }

private:
  enum class LifecyclePhase {
    AwaitRunStart,
    Bootstrap,
    Workload,
    Closed,
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
    std::uint64_t processing_nanoseconds = 0;
  };

  struct Failure {
    CdcErrorCode code;
    std::string message;
    std::string stream_id;
  };

  struct MarkerInsert {
    RunScopedCorrelationId correlation_id;
    std::string session_id;
    std::uint64_t txn_id = 0;
    std::string identity;
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

  void recordAcceptProcessing(
      const std::uint64_t started,
      const std::optional<CdcTransaction> &completed) noexcept {
    const auto finished = monotonic_clock_nanoseconds_();
    const auto duration = finished >= started ? finished - started : 0U;
    accept_total_nanoseconds_ =
        saturatingAdd(accept_total_nanoseconds_, duration);
    accept_max_nanoseconds_ = std::max(accept_max_nanoseconds_, duration);

    if (completed.has_value()) {
      const auto transaction_duration =
          saturatingAdd(completed_processing_before_current_, duration);
      if (completed->kind == CdcTransactionKind::WorkloadMarked) {
        workload_processing_total_nanoseconds_ = saturatingAdd(
            workload_processing_total_nanoseconds_, transaction_duration);
        workload_processing_max_nanoseconds_ = std::max(
            workload_processing_max_nanoseconds_, transaction_duration);
      }
      return;
    }
    if (!current_timed_structural_id_.has_value()) {
      return;
    }
    const auto active = active_.find(*current_timed_structural_id_);
    if (active != active_.end()) {
      active->second.processing_nanoseconds =
          saturatingAdd(active->second.processing_nanoseconds, duration);
    }
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
    if (config_.backend_adapter == nullptr ||
        config_.benchmark_adapter == nullptr) {
      throw std::invalid_argument(
          "assembler backend/benchmark adapter is missing");
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
    const auto register_table = [this,
                                 &all_tables](const QualifiedTable &table) {
      if (!all_tables.insert(table.full_name).second ||
          !tables_by_leaf_.emplace(table.table, table).second) {
        throw std::invalid_argument(
            "assembler table roster is duplicated or has an ambiguous leaf "
            "name: " +
            table.full_name);
      }
    };
    for (const auto &name : config_.application_tables) {
      const auto parsed = parseQualifiedTable(name);
      if (!parsed.has_value()) {
        throw std::invalid_argument(
            "assembler application table is invalid or duplicated: " + name);
      }
      register_table(*parsed);
      application_tables_.emplace(name, *parsed);
    }
    config_.benchmark_adapter->validateApplicationTableRoster(
        config_.application_tables);
    if (!config_.benchmark_adapter->runtimeCapabilities().cdc_normalization) {
      throw MonitorError(
          MonitorErrorCode::BranchContractMismatch,
          "benchmark adapter lacks CDC normalization capability");
    }

    const auto marker = parseQualifiedTable(config_.transaction_marker_table);
    const auto fence = parseQualifiedTable(config_.run_fence_table);
    if (!marker.has_value() || !fence.has_value()) {
      throw std::invalid_argument("assembler control table name is invalid");
    }
    register_table(*marker);
    register_table(*fence);
    marker_table_ = *marker;
    fence_table_ = *fence;
    startup_schema_decoder_ =
        config_.backend_adapter->createCdcStartupSchemaDecoder(
            CdcStartupSchemaConfig{
                .backend_source_id = config_.backend_source_id,
                .debezium_version = config_.debezium_version,
                .application_tables = config_.application_tables,
                .control_tables = {marker->full_name, fence->full_name},
            });
    if (startup_schema_decoder_ == nullptr) {
      throw std::invalid_argument("backend startup schema decoder is missing");
    }
    cdc_decoder_ =
        config_.backend_adapter->createCdcDecoder(BackendCdcDecoderConfig{
            .backend_source_id = config_.backend_source_id,
            .connector_version = config_.debezium_version,
            .application_tables = config_.application_tables,
            .control_tables = {marker->full_name, fence->full_name},
        });
    if (cdc_decoder_ == nullptr) {
      throw std::invalid_argument("backend CDC decoder is missing");
    }
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

  void acceptStartupSchemaRecord(const Json &key, const Json &envelope) {
    constexpr auto code = CdcErrorCode::DdlObserved;
    if (lifecycle_phase_ != LifecyclePhase::AwaitRunStart || !active_.empty() ||
        next_cdc_seq_ != 0U || startup_schema_decoder_->complete()) {
      fail(code, "Debezium schema-change/DDL envelope was observed after "
                 "transactional run evidence began");
    }
    if (startup_schema_snapshot_records_ >=
        config_.max_events_per_transaction) {
      fail(CdcErrorCode::ResourceExhausted,
           "startup schema snapshot record limit exceeded");
    }
    try {
      auto observation = startup_schema_decoder_->accept(key, envelope);
      if (observation.application_table_schema.has_value()) {
        config_.benchmark_adapter->validateStartupTableSchema(
            *observation.application_table_schema,
            config_.backend_adapter->applicationTableRequirements());
      }
    } catch (const AdapterSchemaError &error) {
      fail(code, error.what());
    }
    ++startup_schema_snapshot_records_;
  }

  [[nodiscard]] std::optional<CdcTransaction>
  acceptTransactionMetadata(const CdcStreamRecord &record,
                            const BackendCdcRecord &decoded) {
    if (!startup_schema_decoder_->complete()) {
      if (!startup_schema_decoder_->sawRecord()) {
        fail(CdcErrorCode::CdcRecordMalformed,
             "transactional evidence began without the required startup "
             "schema snapshot");
      }
      fail(CdcErrorCode::DdlObserved,
           "transactional evidence began before the startup schema snapshot "
           "reached its last record");
    }
    if (decoded.kind == BackendCdcRecordKind::Begin) {
      if (!active_.empty()) {
        fail(CdcErrorCode::CdcOrderInvalid,
             "nested or interleaved Debezium BEGIN violates configured "
             "contiguous transaction framing");
      }
      if (active_.size() >= config_.max_active_transactions) {
        fail(CdcErrorCode::ResourceExhausted,
             "active Debezium transaction limit exceeded");
      }
      const auto raw_bytes =
          record.key_json.size() + record.envelope_json.size();
      reserveActiveBytes(raw_bytes);
      ActiveTransaction transaction;
      transaction.structural_id = record.stream_id;
      transaction.begin_stream_id = record.stream_id;
      transaction.raw_bytes = raw_bytes;
      active_.emplace(record.stream_id, std::move(transaction));
      current_timed_structural_id_ = record.stream_id;
      active_transactions_high_water_ =
          std::max(active_transactions_high_water_, active_.size());
      ++begin_metadata_records_;
      return std::nullopt;
    }

    if (decoded.kind != BackendCdcRecordKind::End) {
      fail(
          CdcErrorCode::CdcRecordMalformed,
          "backend decoder returned a non-metadata record to metadata framing");
    }
    if (active_.empty()) {
      fail(CdcErrorCode::CdcBeginMissing,
           "Debezium END has no active contiguous BEGIN frame");
    }
    auto active = active_.begin();
    current_timed_structural_id_ = active->first;
    auto completed = completeTransaction(active, decoded, record.stream_id);
    ++end_metadata_records_;
    return completed;
  }

  void acceptRow(const CdcStreamRecord &record,
                 const BackendCdcRecord &decoded) {
    if (decoded.kind != BackendCdcRecordKind::Row) {
      fail(CdcErrorCode::CdcRecordMalformed,
           "backend decoder returned a non-row record to row assembly");
    }
    if (active_.empty()) {
      fail(CdcErrorCode::CdcBeginMissing,
           "row event has no active contiguous Debezium BEGIN frame");
    }
    auto active = active_.begin();
    current_timed_structural_id_ = active->first;

    QualifiedTable table;
    if (const auto application =
            application_tables_.find(decoded.canonical_table);
        application != application_tables_.end()) {
      table = application->second;
    } else if (decoded.canonical_table == marker_table_.full_name) {
      table = marker_table_;
    } else if (decoded.canonical_table == fence_table_.full_name) {
      table = fence_table_;
    } else {
      fail(CdcErrorCode::UnexpectedTable,
           "decoded canonical table is outside the configured allowlist");
    }
    const bool application = application_tables_.contains(table.full_name);
    const bool marker = table.full_name == marker_table_.full_name;
    const bool fence = table.full_name == fence_table_.full_name;
    if (!application && !marker && !fence) {
      fail(CdcErrorCode::UnexpectedTable,
           "CDC row names a table outside the configured allowlist: " +
               table.full_name);
    }

    const auto total_order = decoded.total_order;
    const auto collection_order = decoded.data_collection_order;
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

    const auto raw_bytes = record.key_json.size() + record.envelope_json.size();
    reserveActiveBytes(raw_bytes);
    active->second.rows.push_back(RowEvent{
        .key = decoded.key,
        .before = decoded.before,
        .after = decoded.after,
        .operation = decoded.operation,
        .table = std::move(table),
        .total_order = total_order,
        .data_collection_order = collection_order,
        .stream_id = record.stream_id,
        .raw_bytes = raw_bytes,
    });
    active->second.table_counts[active->second.rows.back().table.full_name] =
        collection_order;
    active->second.raw_bytes += raw_bytes;
    observed_events_per_transaction_high_water_ =
        std::max(observed_events_per_transaction_high_water_,
                 active->second.rows.size());
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
    retained_identity_high_water_bytes_ =
        std::max(retained_identity_high_water_bytes_, retained_identity_bytes_);
  }

  [[nodiscard]] std::optional<CdcTransaction> completeTransaction(
      const std::unordered_map<std::string, ActiveTransaction>::iterator active,
      const BackendCdcRecord &end, const std::string &end_stream_id) {
    if (!end.event_count.has_value()) {
      fail(CdcErrorCode::CdcEventCountMismatch,
           "decoded END lacks event_count");
    }
    const auto declared_total = *end.event_count;
    if (declared_total != active->second.rows.size()) {
      fail(CdcErrorCode::CdcEventCountMismatch,
           "END.event_count differs from observed row events");
    }
    std::unordered_map<std::string, std::uint64_t> declared_counts;
    std::uint64_t declared_sum = 0;
    for (const auto &collection : end.data_collections) {
      const auto count = collection.event_count;
      if (count == 0U ||
          !declared_counts.emplace(collection.canonical_table, count).second) {
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
    completed_processing_before_current_ =
        active->second.processing_nanoseconds;
    reserveRetainedIdentityBytes(structural_id.size());
    active_.erase(active);
    active_bytes_ -= released_bytes;
    completed_structural_ids_.insert(structural_id);
    retained_identity_high_water_records_ =
        std::max(retained_identity_high_water_records_,
                 completed_structural_ids_.size());
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
      workload_raw_row_events_ = saturatingAdd(
          workload_raw_row_events_, static_cast<std::uint64_t>(raw_row_count));
      workload_normalized_write_events_ =
          saturatingAdd(workload_normalized_write_events_,
                        static_cast<std::uint64_t>(normalized_write_count));
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
      std::vector<const RowEvent *> application_rows;
      application_rows.reserve(transaction.rows.size());
      for (const auto &row : transaction.rows) {
        application_rows.push_back(&row);
      }
      result.writes = normalizeApplicationWrites(application_rows);
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
    std::vector<const RowEvent *> application_rows;
    application_rows.reserve(application_count);
    for (const auto &row : transaction.rows) {
      if (application_tables_.contains(row.table.full_name)) {
        application_rows.push_back(&row);
      }
    }
    result.kind = CdcTransactionKind::WorkloadMarked;
    result.correlation_id = marker.correlation_id;
    result.writes = normalizeApplicationWrites(application_rows);

    if (seen_marker_identities_.contains(marker.identity)) {
      fail(CdcErrorCode::MarkerDuplicate, "unified marker identity was reused");
    }
    reserveRetainedIdentityBytes(marker.identity.size());
    // reserveRetainedIdentityBytes is intentionally before these insertions:
    // an allocation failure is sticky and cannot be mistaken for a valid run.
    seen_marker_identities_.insert(marker.identity);
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

  [[nodiscard]] MarkerInsert validateMarker(const RowEvent &row) const {
    if (row.operation != "c" || !row.before.is_null() ||
        !row.after.is_object()) {
      fail(CdcErrorCode::MarkerMalformed,
           "session_txn marker must be an insert with a null before-image and "
           "complete after-image");
    }
    requireExactKeys(row.key, {"session_id", "txn_id"}, "session_txn key",
                     CdcErrorCode::MarkerMalformed);
    requireExactKeys(row.after, {"session_id", "txn_id"},
                     "session_txn after-image", CdcErrorCode::MarkerMalformed);
    const auto key_session = stringValue(
        member(row.key, "session_id", "session_txn key",
               CdcErrorCode::MarkerMalformed),
        "session_txn key.session_id", CdcErrorCode::MarkerMalformed);
    const auto key_txn =
        uintValue(member(row.key, "txn_id", "session_txn key",
                         CdcErrorCode::MarkerMalformed),
                  "session_txn key.txn_id", kMaxJsonSafeInteger,
                  CdcErrorCode::MarkerMalformed);
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
        key_txn != after_txn || after_txn == 0U) {
      fail(CdcErrorCode::MarkerMalformed,
           "session_txn key/session/txn_id is malformed or inconsistent");
    }

    const std::string identity =
        after_session + '\0' + std::to_string(after_txn);
    if (seen_marker_identities_.contains(identity)) {
      fail(CdcErrorCode::MarkerDuplicate, "unified marker identity was reused");
    }
    return MarkerInsert{
        .correlation_id = RunScopedCorrelationId{.run_id = config_.run_id,
                                                 .session_id = after_session,
                                                 .txn_id = after_txn},
        .session_id = after_session,
        .txn_id = after_txn,
        .identity = identity,
    };
  }

  [[nodiscard]] std::vector<NormalizedWrite>
  normalizeApplicationWrites(const std::vector<const RowEvent *> &rows) const {
    std::map<CanonicalBytes, NormalizedWrite, CanonicalBytesLess> collapsed;
    std::map<CanonicalBytes, std::optional<CanonicalBytes>, CanonicalBytesLess>
        local_state;
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
        [this, &local_state](const CanonicalRowImage &before,
                             const std::string_view operation) {
          const auto previous = local_state.find(before.key);
          if (previous == local_state.end()) {
            return;
          }
          if (!previous->second.has_value()) {
            fail(CdcErrorCode::CdcRecordMalformed,
                 "application " + std::string(operation) +
                     " follows a transaction-local Absent row state");
          }
          if (*previous->second != before.value) {
            fail(CdcErrorCode::CdcRecordMalformed,
                 "application " + std::string(operation) +
                     " before-image does not continue the prior after-image");
          }
        };
    const auto require_insert_follows_absent =
        [this, &local_state](const CanonicalBytes &key) {
          const auto previous = local_state.find(key);
          if (previous != local_state.end() && previous->second.has_value()) {
            fail(CdcErrorCode::CdcRecordMalformed,
                 "application insert follows a transaction-local Present row "
                 "state");
          }
        };

    for (const auto *row_pointer : rows) {
      const auto &row = *row_pointer;
      const auto *codec =
          config_.benchmark_adapter->rowCodec(row.table.full_name);
      if (codec == nullptr) {
        fail(CdcErrorCode::UnexpectedTable,
             "application table has no registered canonical row codec");
      }
      const auto require_primary_columns = [this, &row](const Json &image,
                                                        const char *label) {
        if (!image.is_object()) {
          fail(CdcErrorCode::CdcRecordMalformed,
               std::string(label) + " is not a complete row object");
        }
        const auto *table =
            config_.benchmark_adapter->table(row.table.full_name);
        if (table == nullptr) {
          fail(CdcErrorCode::UnexpectedTable,
               "application image names an unregistered table");
        }
        for (const auto name : table->primary_key) {
          if (!image.contains(name) || image.at(name).is_null()) {
            fail(CdcErrorCode::PrimaryKeyImageIncomplete,
                 std::string(label) +
                     " lacks a non-null primary-key component");
          }
        }
      };
      const auto event_key = [&]() {
        try {
          return codec->encodeKeyObject(row.key);
        } catch (const CanonicalEncodingError &error) {
          fail(CdcErrorCode::PrimaryKeyImageIncomplete,
               std::string("failed to encode application event key: ") +
                   error.what());
        }
      }();
      const auto image = [&](const Json &value, const char *label) {
        require_primary_columns(value, label);
        try {
          return codec->encodeImage(value);
        } catch (const CanonicalEncodingError &error) {
          const auto message = std::string_view(error.what());
          fail(message.starts_with("row_shape:")
                   ? CdcErrorCode::CdcRecordMalformed
                   : CdcErrorCode::CanonicalEncodingFailed,
               std::string("failed to encode ") + label + ": " + error.what());
        }
      };

      if (row.operation == "c") {
        if (!row.before.is_null() || !row.after.is_object()) {
          fail(CdcErrorCode::CdcRecordMalformed,
               "application insert has invalid before/after images");
        }
        const auto after = image(row.after, "insert after-image");
        if (event_key != after.key) {
          fail(CdcErrorCode::PrimaryKeyImageIncomplete,
               "application insert key and after-image disagree");
        }
        require_insert_follows_absent(after.key);
        local_state.insert_or_assign(after.key, after.value);
        put_present(after.key, after.value);
        continue;
      }
      if (row.operation == "d") {
        if (!row.before.is_object() || !row.after.is_null()) {
          fail(CdcErrorCode::CdcRecordMalformed,
               "application delete has invalid before/after images");
        }
        const auto before = image(row.before, "delete before-image");
        if (event_key != before.key) {
          fail(CdcErrorCode::PrimaryKeyImageIncomplete,
               "application delete key and before-image disagree");
        }
        require_before_matches(before, "delete");
        local_state.insert_or_assign(before.key, std::nullopt);
        put_absent(before.key);
        continue;
      }

      if (!row.before.is_object() || !row.after.is_object()) {
        fail(CdcErrorCode::PrimaryKeyImageIncomplete,
             "application update lacks complete before/after images");
      }
      const auto before = image(row.before, "update before-image");
      const auto after = image(row.after, "update after-image");
      if (event_key != after.key) {
        fail(CdcErrorCode::PrimaryKeyImageIncomplete,
             "application update key and after-image disagree");
      }
      require_before_matches(before, "update");
      if (before.key != after.key) {
        require_insert_follows_absent(after.key);
        local_state.insert_or_assign(before.key, std::nullopt);
        put_absent(before.key);
      }
      local_state.insert_or_assign(after.key, after.value);
      put_present(after.key, after.value);
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
  std::function<std::uint64_t()> monotonic_clock_nanoseconds_;
  std::unordered_map<std::string, QualifiedTable> application_tables_;
  std::unordered_map<std::string, QualifiedTable> tables_by_leaf_;
  std::unique_ptr<CdcStartupSchemaDecoder> startup_schema_decoder_;
  std::unique_ptr<BackendCdcDecoder> cdc_decoder_;
  QualifiedTable marker_table_;
  QualifiedTable fence_table_;
  std::unordered_map<std::string, ActiveTransaction> active_;
  std::unordered_set<std::string> completed_structural_ids_;
  std::unordered_set<std::string> seen_marker_identities_;
  std::set<RunFencePhase> seen_fences_;
  LifecyclePhase lifecycle_phase_ = LifecyclePhase::AwaitRunStart;
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
  std::uint64_t workload_raw_row_events_ = 0;
  std::uint64_t normalized_write_events_ = 0;
  std::uint64_t workload_normalized_write_events_ = 0;
  std::uint64_t accept_total_nanoseconds_ = 0;
  std::uint64_t accept_max_nanoseconds_ = 0;
  std::uint64_t workload_processing_total_nanoseconds_ = 0;
  std::uint64_t workload_processing_max_nanoseconds_ = 0;
  std::size_t active_transactions_high_water_ = 0;
  std::size_t observed_events_per_transaction_high_water_ = 0;
  std::size_t active_bytes_ = 0;
  std::size_t active_bytes_high_water_ = 0;
  std::size_t retained_identity_bytes_ = 0;
  std::size_t retained_identity_high_water_records_ = 0;
  std::size_t retained_identity_high_water_bytes_ = 0;
  std::size_t startup_schema_snapshot_records_ = 0;
  std::optional<Failure> failure_;
  std::optional<std::string> current_timed_structural_id_;
  std::uint64_t completed_processing_before_current_ = 0;
  std::string current_stream_id_;
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
