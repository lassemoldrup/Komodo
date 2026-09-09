#include "blackbox/cdc_error.h"
#include "blackbox/debezium_assembler.h"
#include "blackbox/monitor_error.h"
#include "blackbox/protocol_records.h"
#include "blackbox/redis_reader.h"
#include "blackbox/run_controller.h"
#include "workload_rss.h"

#include <hiredis/hiredis.h>
#include <nlohmann/json.hpp>

#include <fcntl.h>
#include <sys/resource.h>
#include <unistd.h>
#if defined(__GLIBC__)
#include <malloc.h>
#endif

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using blackbox::BlockingRedisReader;
using blackbox::CdcAssemblerError;
using blackbox::CdcStreamRecord;
using blackbox::DebeziumAssemblerConfig;
using blackbox::DebeziumTxnAssembler;
using blackbox::DiagnosticCategory;
using blackbox::FinalOutcome;
using blackbox::MonitorError;
using blackbox::ProcessingMode;
using blackbox::ProtocolRecordKind;
using blackbox::ProtocolRecordParser;
using blackbox::RedisReaderError;
using blackbox::RedisReaderErrorCode;
using blackbox::RedisStreamRecord;
using blackbox::RedisStreamSpec;
using blackbox::RunController;
using blackbox::RunManifest;
using Json = nlohmann::json;

class StatusPublicationError final : public std::runtime_error {
public:
  using std::runtime_error::runtime_error;
};

struct Options {
  std::string redis_host = "host.docker.internal";
  int redis_port = 6379;
  std::string run_id;
  std::chrono::milliseconds overall_timeout{120000};
  ProcessingMode processing_mode = ProcessingMode::Audited;
  bool history_gc = false;
  bool workload_rss_sampling = false;
  std::optional<std::string> metrics_output;
  std::vector<blackbox::ProtocolBackendFact> expected_backend_facts;
  std::size_t max_assembler_active_transactions =
      blackbox::DebeziumAssemblerConfig{}.max_active_transactions;
  std::size_t max_assembler_events_per_transaction =
      blackbox::DebeziumAssemblerConfig{}.max_events_per_transaction;
  std::size_t max_assembler_active_bytes =
      blackbox::DebeziumAssemblerConfig{}.max_active_bytes;
  std::size_t max_assembler_completed_transactions =
      blackbox::DebeziumAssemblerConfig{}.max_completed_transactions;
  std::size_t max_pending_client_records =
      blackbox::TransactionJoinerLimits{}.max_pending_client_records;
  std::size_t max_pending_client_bytes =
      blackbox::TransactionJoinerLimits{}.max_pending_client_bytes;
  std::size_t max_pending_cdc_slots =
      blackbox::TransactionJoinerLimits{}.max_pending_cdc_slots;
  std::size_t max_pending_cdc_bytes =
      blackbox::TransactionJoinerLimits{}.max_pending_cdc_bytes;
  std::size_t max_retained_identity_records =
      blackbox::TransactionJoinerLimits{}.max_retained_identity_records;
  std::size_t max_retained_identity_bytes =
      blackbox::TransactionJoinerLimits{}.max_retained_identity_bytes;
  std::size_t max_events_per_transaction =
      blackbox::TransactionJoinerLimits{}.max_events_per_transaction;
  std::size_t max_bootstrap_state_rows =
      blackbox::TransactionJoinerLimits{}.max_bootstrap_state_rows;
  std::size_t max_bootstrap_state_bytes =
      blackbox::TransactionJoinerLimits{}.max_bootstrap_state_bytes;
  std::size_t max_checker_state_bytes =
      blackbox::TransactionJoinerLimits{}.max_checker_state_bytes;
};

[[nodiscard]] bool isLowerUuid(const std::string_view value) {
  if (value.size() != 36U) {
    return false;
  }
  for (std::size_t index = 0; index < value.size(); ++index) {
    if (index == 8U || index == 13U || index == 18U || index == 23U) {
      if (value[index] != '-') {
        return false;
      }
      continue;
    }
    if (!(value[index] >= '0' && value[index] <= '9') &&
        !(value[index] >= 'a' && value[index] <= 'f')) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] int parsePort(const std::string_view value) {
  int result = 0;
  const auto [end, error] =
      std::from_chars(value.data(), value.data() + value.size(), result);
  if (error != std::errc{} || end != value.data() + value.size() ||
      result <= 0 || result > 65535) {
    throw std::invalid_argument("Redis port is invalid");
  }
  return result;
}

[[nodiscard]] std::chrono::milliseconds
parseTimeout(const std::string_view value) {
  std::int64_t result = 0;
  const auto [end, error] =
      std::from_chars(value.data(), value.data() + value.size(), result);
  if (error != std::errc{} || end != value.data() + value.size() ||
      result <= 0) {
    throw std::invalid_argument("overall timeout is invalid");
  }
  return std::chrono::milliseconds(result);
}

[[nodiscard]] std::size_t parsePositiveSize(const std::string_view value,
                                            const std::string_view label) {
  std::size_t result = 0;
  const auto [end, error] =
      std::from_chars(value.data(), value.data() + value.size(), result);
  if (error != std::errc{} || end != value.data() + value.size() ||
      result == 0U) {
    throw std::invalid_argument(std::string(label) +
                                " must be a positive decimal count");
  }
  return result;
}

[[nodiscard]] blackbox::ProtocolBackendFact
parseExpectedBackendFact(const std::string_view value) {
  const auto separator = value.find('=');
  if (separator == std::string_view::npos || separator == 0U ||
      separator + 1U == value.size() || separator > 63U ||
      value.size() - separator - 1U > 4096U) {
    throw std::invalid_argument("expected backend fact must be name=value");
  }
  const auto name = value.substr(0U, separator);
  if (!((name.front() >= 'A' && name.front() <= 'Z') ||
        (name.front() >= 'a' && name.front() <= 'z')) ||
      std::any_of(name.begin() + 1, name.end(), [](const char character) {
        return !((character >= 'A' && character <= 'Z') ||
                 (character >= 'a' && character <= 'z') ||
                 (character >= '0' && character <= '9') || character == '_');
      })) {
    throw std::invalid_argument("expected backend fact name is invalid");
  }
  return blackbox::ProtocolBackendFact{
      .name = std::string(name),
      .canonical_value = std::string(value.substr(separator + 1U)),
  };
}

[[nodiscard]] Options parseOptions(const int argc, char **argv) {
  Options options;
  for (int index = 1; index < argc; ++index) {
    const std::string_view argument(argv[index]);
    if (index + 1 >= argc) {
      throw std::invalid_argument("missing value after " +
                                  std::string(argument));
    }
    const std::string value(argv[++index]);
    if (argument == "--redis-host") {
      options.redis_host = value;
    } else if (argument == "--redis-port") {
      options.redis_port = parsePort(value);
    } else if (argument == "--run-id") {
      options.run_id = value;
    } else if (argument == "--overall-timeout-ms") {
      options.overall_timeout = parseTimeout(value);
    } else if (argument == "--processing-mode") {
      if (value == "audited") {
        options.processing_mode = ProcessingMode::Audited;
      } else if (value == "join-only") {
        options.processing_mode = ProcessingMode::JoinOnly;
      } else {
        throw std::invalid_argument(
            "processing mode must be audited or join-only");
      }
    } else if (argument == "--history-gc") {
      if (value != "on" && value != "off") {
        throw std::invalid_argument("history-gc must be on or off");
      }
      options.history_gc = value == "on";
    } else if (argument == "--workload-rss-sampling") {
      if (value != "on" && value != "off") {
        throw std::invalid_argument("workload-rss-sampling must be on or off");
      }
      options.workload_rss_sampling = value == "on";
    } else if (argument == "--metrics-output") {
      if (value.empty() || value.size() > 4096U) {
        throw std::invalid_argument("metrics output path is invalid");
      }
      options.metrics_output = value;
    } else if (argument == "--expected-backend-fact") {
      options.expected_backend_facts.push_back(parseExpectedBackendFact(value));
    } else if (argument == "--max-assembler-active-transactions") {
      options.max_assembler_active_transactions =
          parsePositiveSize(value, "max assembler active transactions");
    } else if (argument == "--max-assembler-events-per-transaction") {
      options.max_assembler_events_per_transaction =
          parsePositiveSize(value, "max assembler events per transaction");
    } else if (argument == "--max-assembler-active-bytes") {
      options.max_assembler_active_bytes =
          parsePositiveSize(value, "max assembler active bytes");
    } else if (argument == "--max-assembler-completed-transactions") {
      options.max_assembler_completed_transactions =
          parsePositiveSize(value, "max assembler completed transactions");
    } else if (argument == "--max-pending-client-records") {
      options.max_pending_client_records =
          parsePositiveSize(value, "max pending client records");
    } else if (argument == "--max-pending-client-bytes") {
      options.max_pending_client_bytes =
          parsePositiveSize(value, "max pending client bytes");
    } else if (argument == "--max-pending-cdc-slots") {
      options.max_pending_cdc_slots =
          parsePositiveSize(value, "max pending CDC slots");
    } else if (argument == "--max-pending-cdc-bytes") {
      options.max_pending_cdc_bytes =
          parsePositiveSize(value, "max pending CDC bytes");
    } else if (argument == "--max-retained-identity-records") {
      options.max_retained_identity_records =
          parsePositiveSize(value, "max retained identity records");
    } else if (argument == "--max-retained-identity-bytes") {
      options.max_retained_identity_bytes =
          parsePositiveSize(value, "max retained identity bytes");
    } else if (argument == "--max-events-per-transaction") {
      options.max_events_per_transaction =
          parsePositiveSize(value, "max events per transaction");
    } else if (argument == "--max-bootstrap-state-rows") {
      options.max_bootstrap_state_rows =
          parsePositiveSize(value, "max bootstrap state rows");
    } else if (argument == "--max-bootstrap-state-bytes") {
      options.max_bootstrap_state_bytes =
          parsePositiveSize(value, "max bootstrap state bytes");
    } else if (argument == "--max-checker-state-bytes") {
      options.max_checker_state_bytes =
          parsePositiveSize(value, "max checker state bytes");
    } else {
      throw std::invalid_argument("unknown argument: " + std::string(argument));
    }
  }
  if (options.redis_host.empty() || !isLowerUuid(options.run_id)) {
    throw std::invalid_argument(
        "Redis host and a lowercase run UUID are required");
  }
  if (options.processing_mode == ProcessingMode::JoinOnly &&
      !options.metrics_output.has_value()) {
    throw std::invalid_argument(
        "join-only processing requires --metrics-output");
  }
  if (options.workload_rss_sampling && !options.metrics_output.has_value()) {
    throw std::invalid_argument("workload RSS sampling requires --metrics-output");
  }
  return options;
}

struct RedisContextDeleter {
  void operator()(redisContext *context) const noexcept {
    if (context != nullptr) {
      redisFree(context);
    }
  }
};

struct RedisReplyDeleter {
  void operator()(redisReply *reply) const noexcept {
    if (reply != nullptr) {
      freeReplyObject(reply);
    }
  }
};

using RedisContextPtr = std::unique_ptr<redisContext, RedisContextDeleter>;
using RedisReplyPtr = std::unique_ptr<redisReply, RedisReplyDeleter>;

constexpr std::chrono::milliseconds kRedisOperationTimeoutCap{1000};
constexpr std::chrono::milliseconds kTerminalPublicationReserveCap{2000};

[[nodiscard]] timeval
redisTimeoutUntil(const std::chrono::steady_clock::time_point deadline,
                  const std::string_view operation) {
  const auto now = std::chrono::steady_clock::now();
  if (now >= deadline) {
    throw std::runtime_error(std::string(operation) +
                             " exceeded the overall monitor deadline");
  }
  const auto remaining = deadline - now;
  auto timeout =
      std::min(std::chrono::duration_cast<std::chrono::microseconds>(remaining),
               std::chrono::duration_cast<std::chrono::microseconds>(
                   kRedisOperationTimeoutCap));
  if (timeout.count() <= 0) {
    timeout = std::chrono::microseconds(1);
  }
  constexpr auto microseconds_per_second = 1000000;
  return timeval{
      .tv_sec = static_cast<decltype(timeval::tv_sec)>(timeout.count() /
                                                       microseconds_per_second),
      .tv_usec = static_cast<decltype(timeval::tv_usec)>(
          timeout.count() % microseconds_per_second),
  };
}

void setRedisIoDeadline(redisContext *context,
                        const std::chrono::steady_clock::time_point deadline,
                        const std::string_view operation) {
  if (context == nullptr) {
    throw std::runtime_error(std::string(operation) + " has no Redis context");
  }
  const timeval timeout = redisTimeoutUntil(deadline, operation);
  if (redisSetTimeout(context, timeout) != REDIS_OK) {
    const std::string detail = context->err == 0
                                   ? std::string("no hiredis error detail")
                                   : std::string(context->errstr);
    throw std::runtime_error(
        std::string(operation) +
        " could not set its Redis I/O deadline: " + detail);
  }
}

void setReaderDeadline(redisContext *context,
                       const std::chrono::steady_clock::time_point deadline,
                       const std::string_view operation) {
  try {
    setRedisIoDeadline(context, deadline, operation);
  } catch (...) {
    std::throw_with_nested(RedisReaderError(
        RedisReaderErrorCode::ConnectionFailed,
        std::string(operation) + " could not establish a bounded Redis I/O"));
  }
}

[[nodiscard]] RedisContextPtr
connectRedis(const Options &options,
             const std::chrono::steady_clock::time_point deadline) {
  const timeval timeout = redisTimeoutUntil(deadline, "Redis connection");
  RedisContextPtr context(redisConnectWithTimeout(options.redis_host.c_str(),
                                                  options.redis_port, timeout));
  if (!context) {
    throw std::runtime_error("hiredis could not allocate a Redis context");
  }
  if (context->err != 0) {
    throw std::runtime_error(std::string("Redis connection failed: ") +
                             context->errstr);
  }
  setRedisIoDeadline(context.get(), deadline, "Redis connection");
  return context;
}

[[nodiscard]] bool isStreamId(const std::string_view value) {
  const auto separator = value.find('-');
  if (separator == std::string_view::npos || separator == 0U ||
      separator + 1U == value.size()) {
    return false;
  }
  const auto component = [](const std::string_view text) {
    if (text.empty() || (text.size() > 1U && text.front() == '0')) {
      return false;
    }
    return text.find_first_not_of("0123456789") == std::string_view::npos;
  };
  return component(value.substr(0U, separator)) &&
         component(value.substr(separator + 1U));
}

class StatusWriter final {
public:
  StatusWriter(redisContext *context, std::string stream,
               const std::chrono::steady_clock::time_point deadline)
      : context_(context), stream_(std::move(stream)), deadline_(deadline) {
    if (context_ == nullptr || stream_.empty()) {
      throw std::invalid_argument("status writer configuration is invalid");
    }
  }

  [[nodiscard]] std::string publish(const std::string_view payload) const {
    try {
      setRedisIoDeadline(context_, deadline_, "monitor status XADD");
    } catch (const std::exception &error) {
      throw StatusPublicationError(error.what());
    }
    RedisReplyPtr reply(static_cast<redisReply *>(
        redisCommand(context_, "XADD %b * payload %b", stream_.data(),
                     stream_.size(), payload.data(), payload.size())));
    if (!reply) {
      throw StatusPublicationError("Redis did not acknowledge monitor status");
    }
    if (reply->type != REDIS_REPLY_STRING || reply->str == nullptr) {
      const std::string detail =
          reply->type == REDIS_REPLY_ERROR && reply->str != nullptr
              ? std::string(reply->str, reply->len)
              : "unexpected Redis reply type";
      throw StatusPublicationError("Redis rejected monitor status: " + detail);
    }
    const std::string stream_id(reply->str, reply->len);
    if (!isStreamId(stream_id)) {
      throw StatusPublicationError(
          "Redis returned an invalid monitor status stream ID");
    }
    return stream_id;
  }

private:
  redisContext *context_;
  std::string stream_;
  std::chrono::steady_clock::time_point deadline_;
};

[[nodiscard]] bool
claimRunOwnership(redisContext *context, const Options &options,
                  const std::string_view status_stream,
                  const std::chrono::steady_clock::time_point deadline) {
  constexpr std::string_view script =
      "if redis.call('EXISTS', KEYS[1], KEYS[2]) ~= 0 then return 0 end "
      "redis.call('SET', KEYS[1], ARGV[1]) return 1";
  const std::string owner_key = "blackbox:monitor-owner:" + options.run_id;
  setRedisIoDeadline(context, deadline, "monitor ownership claim");
  RedisReplyPtr reply(static_cast<redisReply *>(redisCommand(
      context, "EVAL %b 2 %b %b %b", script.data(), script.size(),
      owner_key.data(), owner_key.size(), status_stream.data(),
      status_stream.size(), options.run_id.data(), options.run_id.size())));
  if (!reply) {
    throw std::runtime_error(
        "Redis did not acknowledge the monitor ownership claim");
  }
  if (reply->type == REDIS_REPLY_INTEGER && reply->integer == 1) {
    return true;
  }
  if (reply->type == REDIS_REPLY_INTEGER && reply->integer == 0) {
    return false;
  }
  const std::string detail =
      reply->type == REDIS_REPLY_ERROR && reply->str != nullptr
          ? std::string(reply->str, reply->len)
          : "unexpected Redis reply type";
  throw std::runtime_error("Redis rejected the monitor ownership claim: " +
                           detail);
}

[[nodiscard]] std::string boundedMessage(std::string message) {
  constexpr std::size_t maximum = 4096U;
  if (message.empty()) {
    return "unspecified monitor failure";
  }
  if (message.size() > maximum) {
    message.resize(maximum);
  }
  return message;
}

[[nodiscard]] std::string
repeatedReadModeName(const co_checker::online::RepeatedReadProbeMode mode) {
  switch (mode) {
  case co_checker::online::RepeatedReadProbeMode::Scalar:
    return "scalar";
  case co_checker::online::RepeatedReadProbeMode::SSE2:
    return "sse2";
  case co_checker::online::RepeatedReadProbeMode::AVX2:
    return "avx2";
  case co_checker::online::RepeatedReadProbeMode::NEON:
    return "neon";
  }
  throw std::logic_error("unknown repeated-read probe mode");
}

[[nodiscard]] Json
assemblerMetricsJson(const blackbox::DebeziumAssemblerMetrics &metrics) {
  return Json{
      {"accepted_raw_records", metrics.accepted_raw_records},
      {"accepted_raw_bytes", metrics.accepted_raw_bytes},
      {"startup_schema_records", metrics.startup_schema_records},
      {"begin_metadata_records", metrics.begin_metadata_records},
      {"end_metadata_records", metrics.end_metadata_records},
      {"row_records", metrics.row_records},
      {"completed_transactions", metrics.completed_transactions},
      {"completed_run_control_transactions",
       metrics.completed_run_control_transactions},
      {"completed_bootstrap_transactions",
       metrics.completed_bootstrap_transactions},
      {"completed_workload_transactions",
       metrics.completed_workload_transactions},
      {"raw_row_events", metrics.raw_row_events},
      {"workload_raw_row_events", metrics.workload_raw_row_events},
      {"normalized_write_events", metrics.normalized_write_events},
      {"workload_normalized_write_events",
       metrics.workload_normalized_write_events},
      {"accept_total_nanoseconds", metrics.accept_total_nanoseconds},
      {"accept_max_nanoseconds", metrics.accept_max_nanoseconds},
      {"workload_processing_total_nanoseconds",
       metrics.workload_processing_total_nanoseconds},
      {"workload_processing_max_nanoseconds",
       metrics.workload_processing_max_nanoseconds},
      {"active_transactions", metrics.active_transactions},
      {"active_transactions_high_water",
       metrics.active_transactions_high_water},
      {"observed_events_per_transaction_high_water",
       metrics.observed_events_per_transaction_high_water},
      {"active_assembly_bytes", metrics.active_assembly_bytes},
      {"active_assembly_high_water_bytes",
       metrics.active_assembly_high_water_bytes},
      {"retained_identity_records", metrics.retained_identity_records},
      {"retained_identity_bytes", metrics.retained_identity_bytes},
      {"retained_identity_high_water_records",
       metrics.retained_identity_high_water_records},
      {"retained_identity_high_water_bytes",
       metrics.retained_identity_high_water_bytes},
  };
}

[[nodiscard]] Json memoryProfileJson(
    const std::optional<co_checker::online::SnapshotSOWWMemoryProfile> &profile) {
  if (!profile) return nullptr;
  return Json{
      {"snapshot_size_bytes", profile->snapshot_size_bytes},
      {"snapshot_capacity_bytes", profile->snapshot_capacity_bytes},
      {"latest_writer_size_bytes", profile->latest_writer_size_bytes},
      {"latest_writer_capacity_bytes", profile->latest_writer_capacity_bytes},
      {"key_index_capacity_bytes", profile->key_index_capacity_bytes},
      {"key_descriptor_capacity_bytes", profile->key_descriptor_capacity_bytes},
      {"key_payload_capacity_bytes", profile->key_payload_capacity_bytes},
      {"key_free_id_capacity_bytes", profile->key_free_id_capacity_bytes},
      {"deferred_deleted_key_capacity_bytes", profile->deferred_deleted_key_capacity_bytes},
      {"value_index_capacity_bytes", profile->value_index_capacity_bytes},
      {"value_descriptor_capacity_bytes", profile->value_descriptor_capacity_bytes},
      {"value_payload_capacity_bytes", profile->value_payload_capacity_bytes},
      {"value_reference_capacity_bytes", profile->value_reference_capacity_bytes},
      {"value_free_id_capacity_bytes", profile->value_free_id_capacity_bytes},
      {"snapshot_value_count", profile->snapshot_value_count},
      {"snapshot_value_bytes", profile->snapshot_value_bytes},
      {"history_only_value_count", profile->history_only_value_count},
      {"history_only_value_bytes", profile->history_only_value_bytes},
      {"unreferenced_value_count", profile->unreferenced_value_count},
      {"unreferenced_value_bytes", profile->unreferenced_value_bytes},
      {"history_delta_capacity_bytes", profile->history_delta_capacity_bytes},
      {"session_value_bytes", profile->session_value_bytes},
      {"session_bucket_bytes", profile->session_bucket_bytes},
  };
}

[[nodiscard]] Json
checkerMetricsJson(const blackbox::CheckerAdapterMetrics &metrics) {
  const auto &engine = metrics.engine;
  const auto &repeated_reads = engine.repeated_reads;
  return Json{
      {"next_commit_seq", metrics.next_commit_seq},
      {"consumed_transactions", metrics.consumed_transactions},
      {"bootstrap_transactions", metrics.bootstrap_transactions},
      {"phase_anchor_transactions", metrics.phase_anchor_transactions},
      {"workload_transactions", metrics.workload_transactions},
      {"read_events", metrics.read_events},
      {"write_events", metrics.write_events},
      {"consume_total_nanoseconds", metrics.consume_total_nanoseconds},
      {"consume_max_nanoseconds", metrics.consume_max_nanoseconds},
      {"workload_read_events", metrics.workload_read_events},
      {"workload_write_events", metrics.workload_write_events},
      {"workload_consume_total_nanoseconds",
       metrics.workload_consume_total_nanoseconds},
      {"workload_consume_max_nanoseconds",
       metrics.workload_consume_max_nanoseconds},
      {"mapped_identity_records", metrics.mapped_identity_records},
      {"mapped_identity_bytes", metrics.mapped_identity_bytes},
      {"mapped_identity_high_water_records",
       metrics.mapped_identity_high_water_records},
      {"mapped_identity_high_water_bytes",
       metrics.mapped_identity_high_water_bytes},
      {"events_per_transaction_high_water",
       metrics.events_per_transaction_high_water},
      {"history_gc_total_nanoseconds", metrics.history_gc_total_nanoseconds},
      {"engine",
       Json{
           {"memory_profile", memoryProfileJson(engine.memory_profile)},
           {"retained_state_capacity_bytes",
            engine.retained_state_capacity_bytes},
           {"retained_state_bytes", engine.retained_state_bytes},
           {"peak_retained_state_bytes", engine.peak_retained_state_bytes},
           {"interned_key_count", engine.interned_key_count},
           {"interned_key_bytes", engine.interned_key_bytes},
           {"interned_value_count", engine.interned_value_count},
           {"interned_value_bytes", engine.interned_value_bytes},
           {"reclaimed_value_count", engine.reclaimed_value_count},
           {"reclaimed_value_bytes", engine.reclaimed_value_bytes},
           {"reclaimed_key_count", engine.reclaimed_key_count},
           {"reclaimed_key_bytes", engine.reclaimed_key_bytes},
           {"current_snapshot_keys", engine.current_snapshot_keys},
           {"history_entries", engine.history_entries},
           {"history_deltas", engine.history_deltas},
           {"history_logical_bytes", engine.history_logical_bytes},
           {"retained_floor", engine.retained_floor.has_value()
                                  ? Json(*engine.retained_floor)
                                  : Json(nullptr)},
           {"reclaimed_history_entries", engine.reclaimed_history_entries},
           {"reclaimed_history_deltas", engine.reclaimed_history_deltas},
           {"reclaimed_history_bytes", engine.reclaimed_history_bytes},
           {"snapshot_read_events", engine.snapshot_read_events},
           {"unique_snapshot_reads", engine.unique_snapshot_reads},
           {"identical_snapshot_repeats", engine.identical_snapshot_repeats},
           {"conflicting_snapshot_repeats",
            engine.conflicting_snapshot_repeats},
           {"snapshot_read_packed_bytes", engine.snapshot_read_packed_bytes},
           {"duplicate_snapshot_read_packed_bytes",
            engine.duplicate_snapshot_read_packed_bytes},
           {"identical_snapshot_repeat_packed_bytes",
            engine.identical_snapshot_repeat_packed_bytes},
           {"conflicting_snapshot_repeat_packed_bytes",
            engine.conflicting_snapshot_repeat_packed_bytes},
           {"compatible_snapshot_checks", engine.compatible_snapshot_checks},
           {"history_entries_visited", engine.history_entries_visited},
           {"history_deltas_visited", engine.history_deltas_visited},
           {"history_relevant_deltas_visited",
            engine.history_relevant_deltas_visited},
           {"max_history_entries_visited", engine.max_history_entries_visited},
           {"max_history_deltas_visited", engine.max_history_deltas_visited},
           {"repeated_reads",
            Json{
                {"detailed_profiling_enabled",
                 repeated_reads.detailed_profiling_enabled},
                {"size", repeated_reads.size},
                {"capacity", repeated_reads.capacity},
                {"touched_size", repeated_reads.touched_size},
                {"touched_capacity", repeated_reads.touched_capacity},
                {"probe_groups", repeated_reads.probe_groups},
                {"key_comparisons", repeated_reads.key_comparisons},
                {"resizes", repeated_reads.resizes},
                {"shrinks", repeated_reads.shrinks},
                {"shrink_attempts", repeated_reads.shrink_attempts},
                {"shrink_failures", repeated_reads.shrink_failures},
                {"max_shrink_rebuild_ns", repeated_reads.max_shrink_rebuild_ns},
                {"reset_calls", repeated_reads.reset_calls},
                {"reset_expected_events", repeated_reads.reset_expected_events},
                {"max_reset_expected_events",
                 repeated_reads.max_reset_expected_events},
                {"clear_calls", repeated_reads.clear_calls},
                {"clear_slot_visits", repeated_reads.clear_slot_visits},
                {"clear_control_bytes_filled",
                 repeated_reads.clear_control_bytes_filled},
                {"clear_occupied_slots_reset",
                 repeated_reads.clear_occupied_slots_reset},
                {"mode", repeatedReadModeName(repeated_reads.mode)},
            }},
       }},
  };
}

[[nodiscard]] Json
joinerMetricsJson(const blackbox::TransactionJoinerMetrics &metrics) {
  return Json{
      {"next_cdc_seq", metrics.next_cdc_seq},
      {"accepted_client_transactions", metrics.accepted_client_transactions},
      {"accepted_cdc_transactions", metrics.accepted_cdc_transactions},
      {"accepted_control_cdc_transactions",
       metrics.accepted_control_cdc_transactions},
      {"accepted_bootstrap_cdc_transactions",
       metrics.accepted_bootstrap_cdc_transactions},
      {"accepted_workload_cdc_transactions",
       metrics.accepted_workload_cdc_transactions},
      {"released_bootstrap_transactions",
       metrics.released_bootstrap_transactions},
      {"released_phase_anchor_transactions",
       metrics.released_phase_anchor_transactions},
      {"released_workload_transactions",
       metrics.released_workload_transactions},
      {"released_bootstrap_write_events",
       metrics.released_bootstrap_write_events},
      {"released_ordered_events", metrics.released_ordered_events},
      {"joined_workload_transactions", metrics.joined_workload_transactions},
      {"marker_only_workload_transactions",
       metrics.marker_only_workload_transactions},
      {"joined_read_events", metrics.joined_read_events},
      {"joined_write_events", metrics.joined_write_events},
      {"joined_ordered_events", metrics.joined_ordered_events},
      {"workload_processing_total_nanoseconds",
       metrics.workload_processing_total_nanoseconds},
      {"workload_processing_max_nanoseconds",
       metrics.workload_processing_max_nanoseconds},
      {"client_first_matches", metrics.client_first_matches},
      {"cdc_first_matches", metrics.cdc_first_matches},
      {"pending_client_records", metrics.pending_client_records},
      {"pending_client_bytes", metrics.pending_client_bytes},
      {"pending_client_high_water_records",
       metrics.pending_client_high_water_records},
      {"pending_client_high_water_bytes",
       metrics.pending_client_high_water_bytes},
      {"pending_cdc_slots", metrics.pending_cdc_slots},
      {"pending_cdc_bytes", metrics.pending_cdc_bytes},
      {"pending_cdc_high_water_slots", metrics.pending_cdc_high_water_slots},
      {"pending_cdc_high_water_bytes", metrics.pending_cdc_high_water_bytes},
      {"join_buffer_high_water_records",
       metrics.join_buffer_high_water_records},
      {"join_buffer_high_water_bytes", metrics.join_buffer_high_water_bytes},
      {"released_events_per_transaction_high_water",
       metrics.released_events_per_transaction_high_water},
      {"unmatched_client_records", metrics.unmatched_client_records},
      {"unmatched_workload_cdc_slots", metrics.unmatched_workload_cdc_slots},
      {"missing_client_head_wait_count",
       metrics.missing_client_head_wait_count},
      {"missing_client_head_wait_current_nanoseconds",
       metrics.missing_client_head_wait_current_nanoseconds},
      {"missing_client_head_wait_total_nanoseconds",
       metrics.missing_client_head_wait_total_nanoseconds},
      {"missing_client_head_wait_max_nanoseconds",
       metrics.missing_client_head_wait_max_nanoseconds},
      {"retained_identity_records", metrics.retained_identity_records},
      {"retained_identity_bytes", metrics.retained_identity_bytes},
      {"retained_identity_high_water_records",
       metrics.retained_identity_high_water_records},
      {"retained_identity_high_water_bytes",
       metrics.retained_identity_high_water_bytes},
      {"bootstrap_state_rows", metrics.bootstrap_state_rows},
      {"bootstrap_state_bytes", metrics.bootstrap_state_bytes},
      {"bootstrap_state_high_water_rows",
       metrics.bootstrap_state_high_water_rows},
      {"bootstrap_state_high_water_bytes",
       metrics.bootstrap_state_high_water_bytes},
      {"drain_invocations", metrics.drain_invocations},
      {"drain_total_nanoseconds", metrics.drain_total_nanoseconds},
      {"drain_max_nanoseconds", metrics.drain_max_nanoseconds},
      {"end_fence_drain_current_nanoseconds",
       metrics.end_fence_drain_current_nanoseconds},
      {"end_fence_drain_nanoseconds", metrics.end_fence_drain_nanoseconds},
      {"end_fence_observed", metrics.end_fence_observed},
      {"end_fence_processed", metrics.end_fence_processed},
      {"bootstrap_state_validated", metrics.bootstrap_state_validated},
  };
}

[[nodiscard]] Json reconciliationJson(
    const std::optional<blackbox::ReconciliationSummary> &reconciliation) {
  if (!reconciliation.has_value()) {
    return nullptr;
  }
  return Json{
      {"evidence_reconciled", reconciliation->evidence_reconciled},
      {"startup_schema_attested", reconciliation->startup_schema_attested},
      {"final_schema_attested", reconciliation->final_schema_attested},
      {"schema_attestation_digest",
       reconciliation->schema_attestation_digest},
      {"bootstrap_manifest_validated",
       reconciliation->bootstrap_manifest_validated},
      {"end_fence_seen", reconciliation->end_fence_seen},
      {"load_finished_cdc_stream_id",
       reconciliation->load_finished_cdc_stream_id},
      {"end_fence_cdc_stream_id", reconciliation->end_fence_cdc_stream_id},
      {"expected_sessions", reconciliation->expected_sessions},
      {"completed_sessions", reconciliation->completed_sessions},
      {"accepted_client_transactions",
       reconciliation->accepted_client_transactions},
      {"joined_workload_transactions",
       reconciliation->joined_workload_transactions},
      {"bootstrap_cdc_transactions",
       reconciliation->bootstrap_cdc_transactions},
      {"bootstrap_checker_transactions",
       reconciliation->bootstrap_checker_transactions},
      {"phase_anchor_transactions", reconciliation->phase_anchor_transactions},
      {"checker_consumed_transactions",
       reconciliation->checker_consumed_transactions},
      {"unmatched_client_records", reconciliation->unmatched_client_records},
      {"unmatched_workload_cdc_slots",
       reconciliation->unmatched_workload_cdc_slots},
      {"ambiguous_attempts", reconciliation->ambiguous_attempts},
  };
}

[[nodiscard]] Json
diagnosticsJson(const std::vector<blackbox::RunDiagnostic> &diagnostics) {
  Json result = Json::array();
  for (const auto &diagnostic : diagnostics) {
    result.push_back(Json{{"category", blackbox::toString(diagnostic.category)},
                          {"code", diagnostic.code},
                          {"message", diagnostic.message}});
  }
  return result;
}

[[nodiscard]] std::uint64_t timevalNanoseconds(const timeval value) {
  constexpr std::uint64_t nanoseconds_per_second = 1000000000ULL;
  constexpr std::uint64_t nanoseconds_per_microsecond = 1000ULL;
  const auto seconds = static_cast<std::uint64_t>(value.tv_sec);
  const auto microseconds = static_cast<std::uint64_t>(value.tv_usec);
  if (seconds >
      std::numeric_limits<std::uint64_t>::max() / nanoseconds_per_second) {
    return std::numeric_limits<std::uint64_t>::max();
  }
  const auto whole = seconds * nanoseconds_per_second;
  const auto fraction = microseconds * nanoseconds_per_microsecond;
  return fraction > std::numeric_limits<std::uint64_t>::max() - whole
             ? std::numeric_limits<std::uint64_t>::max()
             : whole + fraction;
}

[[nodiscard]] std::uint64_t elapsedUsageNanoseconds(const timeval started,
                                                    const timeval finished) {
  const auto start = timevalNanoseconds(started);
  const auto finish = timevalNanoseconds(finished);
  return finish >= start ? finish - start : 0U;
}

[[nodiscard]] std::string
utcTimestamp(const std::chrono::system_clock::time_point point) {
  const auto milliseconds =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          point.time_since_epoch());
  const auto seconds =
      std::chrono::duration_cast<std::chrono::seconds>(milliseconds);
  const auto fractional = milliseconds - seconds;
  const std::time_t time = std::chrono::system_clock::to_time_t(
      std::chrono::system_clock::time_point(seconds));
  std::tm utc{};
  if (gmtime_r(&time, &utc) == nullptr) {
    throw std::runtime_error("could not format measurement timestamp");
  }
  std::ostringstream result;
  result << std::put_time(&utc, "%Y-%m-%dT%H:%M:%S") << '.' << std::setfill('0')
         << std::setw(3) << fractional.count() << 'Z';
  return result.str();
}

void validateJoinOnlyMeasurement(const Json &value) {
  if (value.is_object()) {
    for (const auto &[key, child] : value.items()) {
      if (key == "outcome" || key == "verdict") {
        throw std::logic_error(
            "join-only measurement contains a forbidden result key");
      }
      validateJoinOnlyMeasurement(child);
    }
    return;
  }
  if (value.is_array()) {
    for (const auto &child : value) {
      validateJoinOnlyMeasurement(child);
    }
    return;
  }
  if (value.is_string()) {
    const auto text = value.get<std::string>();
    if (text == "PASS" || text == "VIOLATION" || text == "INVALID") {
      throw std::logic_error(
          "join-only measurement contains a forbidden result value");
    }
  }
}

void writeAtomically(const std::string &target,
                     const std::string_view payload) {
  constexpr std::size_t maximum_metrics_bytes = 1024U * 1024U;
  if (payload.size() > maximum_metrics_bytes) {
    throw std::runtime_error("performance measurement exceeds its byte limit");
  }
  const auto suffix =
      std::chrono::steady_clock::now().time_since_epoch().count();
  const std::string temporary =
      target + ".tmp." + std::to_string(static_cast<long long>(getpid())) +
      "." + std::to_string(suffix);
  const int descriptor =
      open(temporary.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (descriptor < 0) {
    throw std::runtime_error("cannot create temporary metrics output: " +
                             std::string(std::strerror(errno)));
  }
  bool closed = false;
  try {
    std::size_t written = 0U;
    while (written < payload.size()) {
      const auto result =
          write(descriptor, payload.data() + written, payload.size() - written);
      if (result < 0 && errno == EINTR) {
        continue;
      }
      if (result <= 0) {
        throw std::runtime_error("cannot write metrics output: " +
                                 std::string(std::strerror(errno)));
      }
      written += static_cast<std::size_t>(result);
    }
    if (fsync(descriptor) != 0) {
      throw std::runtime_error("cannot flush metrics output: " +
                               std::string(std::strerror(errno)));
    }
    if (close(descriptor) != 0) {
      throw std::runtime_error("cannot close metrics output: " +
                               std::string(std::strerror(errno)));
    }
    closed = true;
    if (std::rename(temporary.c_str(), target.c_str()) != 0) {
      throw std::runtime_error("cannot install metrics output: " +
                               std::string(std::strerror(errno)));
    }
  } catch (...) {
    if (!closed) {
      (void)close(descriptor);
    }
    (void)unlink(temporary.c_str());
    throw;
  }
}

class MeasurementArtifact final {
public:
  static constexpr auto memory_sample_interval = std::chrono::seconds(1);
  void startWorkload() {
    workload_started_steady_ = std::chrono::steady_clock::now();
    next_memory_sample_ = {};
    if (options_.workload_rss_sampling) workload_rss_.start();
  }
  void stopWorkload() { workload_rss_.stop(); }

  explicit MeasurementArtifact(const Options &options) : options_(options) {
    const char *profile = std::getenv("BLACKBOX_MONITOR_MEMORY_PROFILE");
    memory_profile_enabled_ = profile != nullptr && std::string_view(profile) == "on";
    if (!options_.metrics_output.has_value()) {
      return;
    }
    started_steady_ = std::chrono::steady_clock::now();
    started_system_ = std::chrono::system_clock::now();
    if (memory_profile_enabled_) {
      memory_timeline_output_.open(*options_.metrics_output + ".memory-timeline.jsonl",
                                   std::ios::out | std::ios::trunc);
      if (!memory_timeline_output_) {
        throw std::runtime_error("cannot open memory timeline output");
      }
    }
    if (getrusage(RUSAGE_SELF, &started_usage_) != 0) {
      throw std::runtime_error("cannot read initial process resource usage");
    }
  }

  ~MeasurementArtifact() {
    if (!written_ && options_.metrics_output.has_value()) {
      try {
        if (diagnostics_.empty()) {
          addDiagnostic(DiagnosticCategory::Operational, "INTERNAL_ERROR",
                        "monitor exited before completing measurement output");
        }
        write();
      } catch (...) {
      }
    }
  }

  void sampleMemory(const DebeziumTxnAssembler &assembler,
                    const RunController &controller, const bool force = false) {
    if (!memory_profile_enabled_ || !options_.metrics_output) return;
    const auto now = std::chrono::steady_clock::now();
    if (!force && now < next_memory_sample_) return;
    next_memory_sample_ = now + memory_sample_interval;
    const auto started = std::chrono::steady_clock::now();
    const auto joiner = controller.joinerMetrics();
    const auto assembly = assembler.metrics();
    const auto &engine = joiner.checker.engine;
    Json point{
        {"epoch_ms", std::chrono::duration_cast<std::chrono::milliseconds>(
            (started_system_ + (now - started_steady_)).time_since_epoch()).count()},
        {"elapsed_ms", std::chrono::duration_cast<std::chrono::milliseconds>(now - started_steady_).count()},
        {"stage", controller.terminal() ? "final" : (workload_started_steady_ ? "workload" : "bootstrap")},
        {"workload_elapsed_ms", workload_started_steady_
            ? Json(std::chrono::duration_cast<std::chrono::milliseconds>(now - *workload_started_steady_).count()) : Json(nullptr)},
        {"workload_transactions", joiner.checker.workload_transactions},
        {"value_count", engine.interned_value_count},
        {"value_bytes", engine.interned_value_bytes},
        {"key_count", engine.interned_key_count},
        {"key_bytes", engine.interned_key_bytes},
        {"snapshot_keys", engine.current_snapshot_keys},
        {"reclaimed_key_count", engine.reclaimed_key_count},
        {"pending_client_records", joiner.pending_client_records},
        {"pending_cdc_slots", joiner.pending_cdc_slots},
        {"history_bytes", engine.history_logical_bytes},
        {"bootstrap_copy_bytes", joiner.bootstrap_state_bytes},
        {"checker_identity_bytes", joiner.checker.mapped_identity_bytes},
        {"checker_identity_records", joiner.checker.mapped_identity_records},
        {"joiner_identity_bytes", joiner.retained_identity_bytes},
        {"joiner_identity_records", joiner.retained_identity_records},
        {"assembler_identity_bytes", assembly.retained_identity_bytes},
        {"assembler_identity_records", assembly.retained_identity_records},
    };
    try {
      std::ifstream input("/proc/self/smaps_rollup");
      point["rss_kib"] = WorkloadRssSampler::parseRss(input);
    } catch (...) {
      point["rss_kib"] = nullptr;
      ++memory_profile_sample_errors_;
    }
#if defined(__GLIBC__)
    const auto allocator = mallinfo2();
    point["allocator_arena_bytes"] = allocator.arena;
    point["allocator_in_use_arena_bytes"] = allocator.uordblks;
    point["allocator_free_arena_bytes"] = allocator.fordblks;
    point["allocator_mmap_bytes"] = allocator.hblkhd;
#endif
    memory_timeline_output_ << point.dump() << '\n';
    memory_timeline_output_.flush();
    if (!memory_timeline_output_) {
      throw std::runtime_error("cannot append memory timeline output");
    }
    ++memory_profile_samples_;
    memory_profile_sample_nanoseconds_ +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - started).count();
  }

  void capture(const DebeziumTxnAssembler &assembler,
               const RunController &controller, const bool final_capture = false) {
    if (!options_.metrics_output.has_value()) {
      return;
    }
    assembler_metrics_ = assembler.metrics();
    if (final_capture && memory_profile_enabled_) {
      stopWorkload(); // Exclude the census bitmap and scan from sampled peak RSS.
      sampleMemory(assembler, controller, true);
      const auto started = std::chrono::steady_clock::now();
      joiner_metrics_ = controller.joinerMetrics(true);
      memory_profile_census_nanoseconds_ =
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::steady_clock::now() - started).count();
    } else {
      joiner_metrics_ = controller.joinerMetrics();
    }
    reconciliation_ = controller.finalReconciliation();
    diagnostics_ = controller.diagnostics();
    final_outcome_ = controller.finalOutcome();
    controller_terminal_ = controller.terminal();
  }

  void addDiagnostic(const DiagnosticCategory category, std::string code,
                     std::string message) {
    if (!options_.metrics_output.has_value()) {
      return;
    }
    if (diagnostics_.size() >= 64U) {
      return;
    }
    diagnostics_.push_back(blackbox::RunDiagnostic{
        .category = category,
        .code = std::move(code),
        .message = boundedMessage(std::move(message)),
    });
  }

  void write() {
    stopWorkload();
    if (written_ || !options_.metrics_output.has_value()) {
      written_ = true;
      return;
    }
    rusage finished_usage{};
    if (getrusage(RUSAGE_SELF, &finished_usage) != 0) {
      throw std::runtime_error("cannot read final process resource usage");
    }
    const auto finished_steady = std::chrono::steady_clock::now();
    const auto finished_system = std::chrono::system_clock::now();
    const bool evidence_reconciled =
        reconciliation_.has_value() && reconciliation_->evidence_reconciled;
    const bool completed = controller_terminal_ && evidence_reconciled;
    Json result{
        {"kind", "performance_measurement"},
        {"run_id", options_.run_id},
        {"processing_mode", blackbox::toString(options_.processing_mode)},
        {"completed", completed},
        {"completion_status", completed ? "completed" : "failed"},
        {"evidence_reconciled", evidence_reconciled},
        {"started_at_utc", utcTimestamp(started_system_)},
        {"finished_at_utc", utcTimestamp(finished_system)},
        {"monitor_wall_time_nanoseconds",
         std::chrono::duration_cast<std::chrono::nanoseconds>(finished_steady -
                                                              started_steady_)
             .count()},
        {"process", Json{{"user_cpu_nanoseconds",
                          elapsedUsageNanoseconds(started_usage_.ru_utime,
                                                  finished_usage.ru_utime)},
                         {"system_cpu_nanoseconds",
                          elapsedUsageNanoseconds(started_usage_.ru_stime,
                                                  finished_usage.ru_stime)},
                         {"max_rss_kib", finished_usage.ru_maxrss}}},
        {"limits",
         Json{{"assembler",
               Json{{"max_active_transactions",
                     options_.max_assembler_active_transactions},
                    {"max_events_per_transaction",
                     options_.max_assembler_events_per_transaction},
                    {"max_active_bytes", options_.max_assembler_active_bytes},
                    {"max_completed_transactions",
                     options_.max_assembler_completed_transactions}}},
              {"joiner",
               Json{{"max_pending_client_records",
                     options_.max_pending_client_records},
                    {"max_pending_client_bytes",
                     options_.max_pending_client_bytes},
                    {"max_pending_cdc_slots", options_.max_pending_cdc_slots},
                    {"max_pending_cdc_bytes", options_.max_pending_cdc_bytes},
                    {"max_retained_identity_records",
                     options_.max_retained_identity_records},
                    {"max_retained_identity_bytes",
                     options_.max_retained_identity_bytes},
                    {"max_events_per_transaction",
                     options_.max_events_per_transaction},
                    {"max_bootstrap_state_rows",
                     options_.max_bootstrap_state_rows},
                    {"max_bootstrap_state_bytes",
                     options_.max_bootstrap_state_bytes},
                    {"max_checker_state_bytes",
                     options_.max_checker_state_bytes}}},
              {"checker", Json{{"history_gc", options_.history_gc},
                               {"max_retained_state_bytes",
                                options_.max_checker_state_bytes},
                               {"max_events_per_transaction",
                                options_.max_events_per_transaction}}}}},
        {"assembler", assemblerMetricsJson(assembler_metrics_)},
        {"joiner", joinerMetricsJson(joiner_metrics_)},
        {"checker", checkerMetricsJson(joiner_metrics_.checker)},
        {"reconciliation", reconciliationJson(reconciliation_)},
        {"diagnostics", diagnosticsJson(diagnostics_)},
    };
    auto &process = result["process"];
    result["identity_policy"] = "ordered_unique_input_v1";
    result["memory_profile"] = Json{
        {"enabled", memory_profile_enabled_},
        {"sample_interval_ms", std::chrono::duration_cast<std::chrono::milliseconds>(
                                   memory_sample_interval).count()},
        {"sample_errors", memory_profile_sample_errors_},
        {"sampling_nanoseconds", memory_profile_sample_nanoseconds_},
        {"final_census_nanoseconds", memory_profile_census_nanoseconds_},
        {"timeline_storage", "jsonl"},
        {"sample_count", memory_profile_samples_},
    };
    process["workload_rss_sampling_enabled"] = options_.workload_rss_sampling;
    process["workload_rss_sampling_started"] = workload_rss_.started();
    process["workload_rss_sampling_valid"] = workload_rss_.valid();
    process["workload_sampled_peak_rss_kib"] =
        workload_rss_.valid() ? Json(workload_rss_.peak()) : Json(nullptr);
    process["rss_sample_interval_ms"] =
        options_.workload_rss_sampling ? Json(WorkloadRssSampler::interval_ms) : Json(nullptr);
    process["workload_rss_samples"] = workload_rss_.samples();
    process["workload_rss_sample_errors"] = workload_rss_.errors();
    if (options_.processing_mode == ProcessingMode::Audited &&
        final_outcome_.has_value()) {
      result["outcome"] = blackbox::toString(*final_outcome_);
      const auto first_violation = std::find_if(
          diagnostics_.begin(), diagnostics_.end(),
          [](const blackbox::RunDiagnostic &diagnostic) {
            return diagnostic.category == DiagnosticCategory::Checker &&
                   diagnostic.code == "VIOLATION_OBSERVED";
          });
      if (first_violation != diagnostics_.end()) {
        result["first_violation_diagnostic"] =
            Json{{"category", blackbox::toString(first_violation->category)},
                 {"code", first_violation->code},
                 {"message", first_violation->message}};
      }
    }
    if (options_.processing_mode == ProcessingMode::JoinOnly) {
      validateJoinOnlyMeasurement(result);
    }
    auto payload = result.dump(-1, ' ', false, Json::error_handler_t::replace);
    payload.push_back('\n');
    writeAtomically(*options_.metrics_output, payload);
    written_ = true;
  }

private:
  const Options &options_;
  WorkloadRssSampler workload_rss_;
  bool memory_profile_enabled_ = false;
  std::optional<std::chrono::steady_clock::time_point> workload_started_steady_;
  std::chrono::steady_clock::time_point next_memory_sample_{};
  std::uint64_t memory_profile_samples_ = 0;
  std::ofstream memory_timeline_output_;
  std::uint64_t memory_profile_sample_errors_ = 0;
  std::int64_t memory_profile_sample_nanoseconds_ = 0;
  std::int64_t memory_profile_census_nanoseconds_ = 0;
  std::chrono::steady_clock::time_point started_steady_{};
  std::chrono::system_clock::time_point started_system_{};
  rusage started_usage_{};
  blackbox::DebeziumAssemblerMetrics assembler_metrics_;
  blackbox::TransactionJoinerMetrics joiner_metrics_;
  std::optional<blackbox::ReconciliationSummary> reconciliation_;
  std::vector<blackbox::RunDiagnostic> diagnostics_;
  std::optional<FinalOutcome> final_outcome_;
  bool controller_terminal_ = false;
  bool written_ = false;
};

[[nodiscard]] std::string minimalInvalidStatus(
    const Options &options, const std::string_view last_client_stream_id,
    const std::string_view category, const std::string_view code,
    std::string message, const std::uint64_t expected_sessions = 0U) {
  return Json{
      {"kind", "status"},
      {"run_id", options.run_id},
      {"lifecycle_state", "FINAL"},
      {"outcome", "INVALID"},
      {"checker_observation", "NOT_STARTED"},
      {"last_cdc_stream_id", "0-0"},
      {"last_client_stream_id", last_client_stream_id},
      {"assembled_cdc_transactions", 0},
      {"joined_transactions", 0},
      {"checked_transactions", 0},
      {"pending_cdc_slots", 0},
      {"unmatched_client_records", 0},
      {"diagnostics",
       Json::array({Json{{"category", category},
                         {"code", code},
                         {"message", boundedMessage(std::move(message))}}})},
      {"reconciliation", Json{{"evidence_reconciled", false},
                              {"startup_schema_attested", false},
                              {"final_schema_attested", false},
                              {"schema_attestation_digest", ""},
                              {"bootstrap_manifest_validated", false},
                              {"end_fence_seen", false},
                              {"load_finished_cdc_stream_id", ""},
                              {"end_fence_cdc_stream_id", ""},
                              {"expected_sessions", expected_sessions},
                              {"completed_sessions", 0},
                              {"accepted_client_transactions", 0},
                              {"joined_workload_transactions", 0},
                              {"bootstrap_cdc_transactions", 0},
                              {"bootstrap_checker_transactions", 0},
                              {"phase_anchor_transactions", 0},
                              {"checker_consumed_transactions", 0},
                              {"unmatched_client_records", 0},
                              {"unmatched_workload_cdc_slots", 0},
                              {"ambiguous_attempts", 0}}}}
      .dump();
}

struct ManifestEvidence {
  RunManifest manifest;
  std::string stream_id;
};

[[nodiscard]] ManifestEvidence
readManifest(redisContext *context, const Options &options,
             const std::chrono::steady_clock::time_point deadline) {
  const std::string client_stream = "blackbox:client:" + options.run_id;
  BlockingRedisReader reader(
      context,
      {RedisStreamSpec{.stream_name = client_stream,
                       .initial_cursor = "0-0",
                       .required_field_name = std::string("payload")}},
      1U, std::chrono::milliseconds(100));
  std::optional<RedisStreamRecord> first;
  while (!first.has_value() && std::chrono::steady_clock::now() < deadline) {
    setReaderDeadline(context, deadline, "RunManifest XREAD");
    (void)reader.read_once([&](const RedisStreamRecord &record) {
      first = record;
      return true;
    });
  }
  if (!first.has_value()) {
    throw MonitorError(blackbox::MonitorErrorCode::RunManifestMissing,
                       "timed out before RunManifest publication");
  }

  ProtocolRecordParser parser;
  if (parser.kind(first->value) != ProtocolRecordKind::RunManifest) {
    throw MonitorError(blackbox::MonitorErrorCode::RunManifestMissing,
                       "the first client stream record is not RunManifest");
  }
  auto manifest = parser.parseRunManifest(first->value);
  if (manifest.run_id != options.run_id) {
    throw MonitorError(blackbox::MonitorErrorCode::RunIdMismatch,
                       "RunManifest belongs to another run");
  }
  return ManifestEvidence{.manifest = std::move(manifest),
                          .stream_id = std::move(first->stream_id)};
}

void publishPending(RunController &controller, const StatusWriter &writer,
                    std::optional<std::string> &terminal_payload,
                    MeasurementArtifact &measurement) {
  for (auto &payload : controller.takePendingStatuses()) {
    const auto status = Json::parse(payload);
    if (status.at("lifecycle_state") == "LOAD_FINISHED_SEEN") {
      measurement.startWorkload();
    }
    if (status.at("lifecycle_state") == "FINAL") measurement.stopWorkload();
    (void)writer.publish(payload);
    if (status.at("lifecycle_state") == "FINAL") {
      if (terminal_payload.has_value()) {
        throw std::runtime_error(
            "controller emitted more than one FINAL status");
      }
      terminal_payload = std::move(payload);
    }
  }
  if (controller.terminal()) measurement.stopWorkload();
}

struct ReaderFailure {
  DiagnosticCategory category = DiagnosticCategory::Operational;
  std::string code = "INTERNAL_ERROR";
  std::string message;
};

[[nodiscard]] ReaderFailure
classifyReaderFailure(const RedisReaderError &error) {
  if (error.code() == RedisReaderErrorCode::CallbackFailed) {
    try {
      std::rethrow_if_nested(error);
    } catch (const CdcAssemblerError &nested) {
      return ReaderFailure{.category = DiagnosticCategory::Protocol,
                           .code =
                               std::string(blackbox::toString(nested.code())),
                           .message = nested.what()};
    } catch (const MonitorError &nested) {
      return ReaderFailure{.category = DiagnosticCategory::Protocol,
                           .code =
                               std::string(blackbox::toString(nested.code())),
                           .message = nested.what()};
    } catch (const std::exception &nested) {
      return ReaderFailure{.category = DiagnosticCategory::Operational,
                           .code = "INTERNAL_ERROR",
                           .message = nested.what()};
    } catch (...) {
      return ReaderFailure{.category = DiagnosticCategory::Operational,
                           .code = "INTERNAL_ERROR",
                           .message = error.what()};
    }
  }
  const auto resource = error.code() == RedisReaderErrorCode::ResourceExhausted;
  return ReaderFailure{.category = DiagnosticCategory::Operational,
                       .code = resource ? "RESOURCE_EXHAUSTED"
                                        : "COMPONENT_RESTARTED",
                       .message = error.what()};
}

[[nodiscard]] int exitCode(const std::optional<FinalOutcome> outcome) {
  if (!outcome.has_value()) {
    return 1;
  }
  switch (*outcome) {
  case FinalOutcome::Pass:
    return 0;
  case FinalOutcome::Violation:
    return 3;
  case FinalOutcome::Invalid:
    return 4;
  }
  return 1;
}

[[nodiscard]] int failureExitCode(const Options &options) noexcept {
  return options.processing_mode == ProcessingMode::JoinOnly ? 1 : 4;
}

void printRunFailure(const Options &options, const std::string_view code,
                     const std::string_view message, std::ostream &stream) {
  if (options.processing_mode == ProcessingMode::JoinOnly) {
    stream << "completion_status=failed code=" << code << " message=" << message
           << '\n';
  } else {
    stream << "outcome=INVALID code=" << code << " message=" << message << '\n';
  }
}

int run(const Options &options) {
  MeasurementArtifact measurement(options);
  const auto started = std::chrono::steady_clock::now();
  const auto deadline = started + options.overall_timeout;
  const auto terminal_reserve =
      std::min(kTerminalPublicationReserveCap, options.overall_timeout / 4);
  const auto evidence_deadline = deadline - terminal_reserve;
  const std::string status_stream = "blackbox:status:" + options.run_id;
  RedisContextPtr status_context;
  try {
    status_context = connectRedis(options, deadline);
    if (!claimRunOwnership(status_context.get(), options, status_stream,
                           deadline)) {
      const std::string message =
          "monitor ownership claim rejected because the owner key or status "
          "stream already exists";
      measurement.addDiagnostic(DiagnosticCategory::Operational,
                                "COMPONENT_RESTARTED", message);
      measurement.write();
      printRunFailure(options, "COMPONENT_RESTARTED", message, std::cerr);
      return failureExitCode(options);
    }
  } catch (const std::exception &error) {
    const auto message = boundedMessage(error.what());
    measurement.addDiagnostic(DiagnosticCategory::Operational,
                              "COMPONENT_RESTARTED", message);
    measurement.write();
    printRunFailure(options, "COMPONENT_RESTARTED", message, std::cerr);
    return failureExitCode(options);
  }
  const StatusWriter status_writer(status_context.get(), status_stream,
                                   deadline);
  RedisContextPtr reader_context;
  try {
    reader_context = connectRedis(options, evidence_deadline);
  } catch (const std::exception &error) {
    const auto message = boundedMessage(error.what());
    if (options.processing_mode == ProcessingMode::Audited) {
      const auto payload = minimalInvalidStatus(options, "0-0", "OPERATIONAL",
                                                "COMPONENT_RESTARTED", message);
      (void)status_writer.publish(payload);
    }
    measurement.addDiagnostic(DiagnosticCategory::Operational,
                              "COMPONENT_RESTARTED", message);
    measurement.write();
    printRunFailure(options, "COMPONENT_RESTARTED", message, std::cout);
    return failureExitCode(options);
  }

  ManifestEvidence manifest_evidence;
  try {
    manifest_evidence =
        readManifest(reader_context.get(), options, evidence_deadline);
  } catch (const MonitorError &error) {
    const std::string code(blackbox::toString(error.code()));
    const auto message = boundedMessage(error.what());
    if (options.processing_mode == ProcessingMode::Audited) {
      const auto payload =
          minimalInvalidStatus(options, "0-0", "PROTOCOL", code, message);
      (void)status_writer.publish(payload);
    }
    measurement.addDiagnostic(DiagnosticCategory::Protocol, code, message);
    measurement.write();
    printRunFailure(options, code, message, std::cout);
    return failureExitCode(options);
  } catch (const RedisReaderError &error) {
    const auto failure = classifyReaderFailure(error);
    if (options.processing_mode == ProcessingMode::Audited) {
      const auto payload = minimalInvalidStatus(
          options, "0-0", blackbox::toString(failure.category), failure.code,
          failure.message);
      (void)status_writer.publish(payload);
    }
    measurement.addDiagnostic(failure.category, failure.code, failure.message);
    measurement.write();
    printRunFailure(options, failure.code, failure.message, std::cout);
    return failureExitCode(options);
  }

  const auto &manifest = manifest_evidence.manifest;
  const std::string client_stream = "blackbox:client:" + options.run_id;
  std::optional<DebeziumTxnAssembler> assembler_storage;
  std::optional<RunController> controller_storage;
  std::unique_ptr<BlockingRedisReader> reader_storage;
  try {
    blackbox::RunControllerConfig controller_config;
    controller_config.processing_mode = options.processing_mode;
    controller_config.expected_backend_facts = options.expected_backend_facts;
    controller_config.joiner_limits.max_pending_client_records =
        options.max_pending_client_records;
    controller_config.joiner_limits.max_pending_client_bytes =
        options.max_pending_client_bytes;
    controller_config.joiner_limits.max_pending_cdc_slots =
        options.max_pending_cdc_slots;
    controller_config.joiner_limits.max_pending_cdc_bytes =
        options.max_pending_cdc_bytes;
    controller_config.joiner_limits.max_retained_identity_records =
        options.max_retained_identity_records;
    controller_config.joiner_limits.max_retained_identity_bytes =
        options.max_retained_identity_bytes;
    controller_config.joiner_limits.max_events_per_transaction =
        options.max_events_per_transaction;
    controller_config.joiner_limits.max_bootstrap_state_rows =
        options.max_bootstrap_state_rows;
    controller_config.joiner_limits.max_bootstrap_state_bytes =
        options.max_bootstrap_state_bytes;
    controller_config.joiner_limits.max_checker_state_bytes =
        options.max_checker_state_bytes;
    controller_config.history_gc = options.history_gc;
    controller_storage.emplace(manifest, std::move(controller_config));
    controller_storage->setLastClientStreamId(manifest_evidence.stream_id);
    assembler_storage.emplace(DebeziumAssemblerConfig{
        .run_id = manifest.run_id,
        .backend_source_id = manifest.backend_source_id,
        .debezium_version = std::string(
            blackbox::BackendAdapterRegistry::require(manifest.backend)
                .connectorVersion()),
        .backend_adapter =
            &blackbox::BackendAdapterRegistry::require(manifest.backend),
        .benchmark_adapter =
            &blackbox::BenchmarkAdapterRegistry::require(manifest.benchmark),
        .application_tables = manifest.application_table_allowlist,
        .transaction_marker_table = manifest.transaction_marker_table,
        .run_fence_table = manifest.control_table_allowlist.front(),
        .max_active_transactions = options.max_assembler_active_transactions,
        .max_events_per_transaction =
            options.max_assembler_events_per_transaction,
        .max_active_bytes = options.max_assembler_active_bytes,
        .max_completed_transactions =
            options.max_assembler_completed_transactions,
    });
    reader_storage = std::make_unique<BlockingRedisReader>(
        reader_context.get(),
        std::vector<RedisStreamSpec>{
            RedisStreamSpec{.stream_name = "cdc",
                            .initial_cursor = "0-0",
                            .required_field_name = std::nullopt},
            RedisStreamSpec{.stream_name = client_stream,
                            .initial_cursor = manifest_evidence.stream_id,
                            .required_field_name = std::string("payload")}},
        1000U, std::chrono::milliseconds(100));
  } catch (const MonitorError &error) {
    const std::string code(blackbox::toString(error.code()));
    const auto message = boundedMessage(error.what());
    if (options.processing_mode == ProcessingMode::Audited) {
      const auto payload = minimalInvalidStatus(
          options, manifest_evidence.stream_id, "PROTOCOL", code, message,
          static_cast<std::uint64_t>(manifest.expected_session_ids.size()));
      (void)status_writer.publish(payload);
    }
    measurement.addDiagnostic(DiagnosticCategory::Protocol, code, message);
    measurement.write();
    printRunFailure(options, code, message, std::cout);
    return failureExitCode(options);
  } catch (const RedisReaderError &error) {
    const auto failure = classifyReaderFailure(error);
    if (options.processing_mode == ProcessingMode::Audited) {
      const auto payload = minimalInvalidStatus(
          options, manifest_evidence.stream_id,
          blackbox::toString(failure.category), failure.code, failure.message,
          static_cast<std::uint64_t>(manifest.expected_session_ids.size()));
      (void)status_writer.publish(payload);
    }
    measurement.addDiagnostic(failure.category, failure.code, failure.message);
    measurement.write();
    printRunFailure(options, failure.code, failure.message, std::cout);
    return failureExitCode(options);
  } catch (const std::bad_alloc &) {
    const std::string message = "monitor construction allocation failed";
    if (options.processing_mode == ProcessingMode::Audited) {
      const auto payload = minimalInvalidStatus(
          options, manifest_evidence.stream_id, "OPERATIONAL",
          "RESOURCE_EXHAUSTED", message,
          static_cast<std::uint64_t>(manifest.expected_session_ids.size()));
      (void)status_writer.publish(payload);
    }
    measurement.addDiagnostic(DiagnosticCategory::Operational,
                              "RESOURCE_EXHAUSTED", message);
    measurement.write();
    printRunFailure(options, "RESOURCE_EXHAUSTED", message, std::cout);
    return failureExitCode(options);
  } catch (const std::exception &error) {
    const auto message = boundedMessage(error.what());
    if (options.processing_mode == ProcessingMode::Audited) {
      const auto payload = minimalInvalidStatus(
          options, manifest_evidence.stream_id, "OPERATIONAL", "INTERNAL_ERROR",
          message,
          static_cast<std::uint64_t>(manifest.expected_session_ids.size()));
      (void)status_writer.publish(payload);
    }
    measurement.addDiagnostic(DiagnosticCategory::Operational, "INTERNAL_ERROR",
                              message);
    measurement.write();
    printRunFailure(options, "INTERNAL_ERROR", message, std::cout);
    return failureExitCode(options);
  }

  auto &assembler = *assembler_storage;
  auto &controller = *controller_storage;
  auto &reader = *reader_storage;
  // Stop sampling before the controller/assembler are destroyed on exceptions.
  struct SamplingScope {
    MeasurementArtifact &measurement;
    ~SamplingScope() { measurement.stopWorkload(); }
  } sampling_scope{measurement};
  std::optional<std::string> terminal_payload;
  publishPending(controller, status_writer, terminal_payload, measurement);

  while (!controller.terminal() &&
         std::chrono::steady_clock::now() < evidence_deadline) {
    try {
      setReaderDeadline(reader_context.get(), evidence_deadline,
                        "evidence XREAD");
      const auto count = reader.read_once([&](const RedisStreamRecord &record) {
        if (record.stream_name == client_stream) {
          controller.setLastClientStreamId(record.stream_id);
          controller.acceptClientPayload(record.value);
        } else {
          controller.setLastCdcStreamId(record.stream_id);
          auto completed = assembler.accept(CdcStreamRecord{
              .stream_id = record.stream_id,
              .key_json = record.field_name,
              .envelope_json = record.value,
          });
          if (completed.has_value()) {
            controller.acceptCdcTransaction(std::move(*completed));
          }
        }
        return true;
      });
      publishPending(controller, status_writer, terminal_payload, measurement);
      if (count == 0U) {
        // END_FENCE is committed only after coordinator-proven client and
        // database-connection quiescence. Observing its completed CDC END is
        // therefore the database-prefix closure proof; this nil read is not a
        // timing substitute for that proof. It ensures the validated XREAD
        // batch and any client-stream backlog visible at closure have both
        // drained before the controller emits its one terminal record.
        if (controller.endFenceObserved()) {
          assembler.finish();
        }
        controller.onInputIdle();
        publishPending(controller, status_writer, terminal_payload, measurement);
      }
      if (!controller.terminal()) measurement.sampleMemory(assembler, controller);
    } catch (const RedisReaderError &error) {
      if (std::chrono::steady_clock::now() >= evidence_deadline) {
        try {
          assembler.finish();
        } catch (const CdcAssemblerError &assembler_error) {
          controller.noteExternalFailure(
              DiagnosticCategory::Protocol,
              std::string(blackbox::toString(assembler_error.code())),
              assembler_error.what());
        }
        controller.forceTimeoutFinal();
        measurement.capture(assembler, controller);
        publishPending(controller, status_writer, terminal_payload, measurement);
        continue;
      }
      const auto failure = classifyReaderFailure(error);
      controller.noteExternalFailure(failure.category, failure.code,
                                     failure.message);
      controller.forceInvalidFinal(failure.code, failure.message);
      measurement.capture(assembler, controller);
      publishPending(controller, status_writer, terminal_payload, measurement);
    } catch (const CdcAssemblerError &error) {
      controller.noteExternalFailure(
          DiagnosticCategory::Protocol,
          std::string(blackbox::toString(error.code())), error.what());
      controller.forceInvalidFinal(
          std::string(blackbox::toString(error.code())), error.what());
      measurement.capture(assembler, controller);
      publishPending(controller, status_writer, terminal_payload, measurement);
    }
  }

  if (!controller.terminal()) {
    try {
      assembler.finish();
    } catch (const CdcAssemblerError &error) {
      controller.noteExternalFailure(
          DiagnosticCategory::Protocol,
          std::string(blackbox::toString(error.code())), error.what());
    }
    controller.forceTimeoutFinal();
    measurement.capture(assembler, controller);
    publishPending(controller, status_writer, terminal_payload, measurement);
  }

  measurement.capture(assembler, controller, true);
  if (options.processing_mode == ProcessingMode::JoinOnly) {
    if (terminal_payload.has_value()) {
      throw std::runtime_error(
          "join-only controller emitted a terminal status record");
    }
    try {
      measurement.write();
    } catch (const std::exception &error) {
      std::cerr << "completion_status=failed code=INTERNAL_ERROR message="
                << error.what() << '\n';
      return 1;
    }
    std::cout << controller.finalReport() << '\n';
    return controller.finalReconciliation().has_value() &&
                   controller.finalReconciliation()->evidence_reconciled
               ? 0
               : 1;
  }
  if (!terminal_payload.has_value()) {
    throw std::runtime_error("controller terminated without one FINAL status");
  }
  try {
    measurement.write();
  } catch (const std::exception &error) {
    std::cerr << "performance_measurement_status=failed code=INTERNAL_ERROR "
                 "message="
              << error.what() << '\n';
    return 1;
  }
  std::cout << controller.finalReport() << '\n';
  return exitCode(controller.finalOutcome());
}

} // namespace

int main(const int argc, char **argv) {
  Options options;
  try {
    options = parseOptions(argc, argv);
  } catch (const std::exception &error) {
    std::cerr << "black-box monitor configuration failed: " << error.what()
              << '\n';
    return 1;
  }
  try {
    return run(options);
  } catch (const StatusPublicationError &error) {
    printRunFailure(options, "REDIS_PUBLICATION_FAILED", error.what(),
                    std::cerr);
    return failureExitCode(options);
  } catch (const std::bad_alloc &) {
    printRunFailure(options, "RESOURCE_EXHAUSTED", "monitor allocation failed",
                    std::cerr);
    return failureExitCode(options);
  } catch (const std::exception &error) {
    printRunFailure(options, "INTERNAL_ERROR", error.what(), std::cerr);
    return failureExitCode(options);
  }
}
