#include "blackbox/cdc_error.h"
#include "blackbox/debezium_assembler.h"
#include "blackbox/monitor_error.h"
#include "blackbox/protocol_records.h"
#include "blackbox/redis_reader.h"
#include "blackbox/run_controller.h"

#include <hiredis/hiredis.h>
#include <nlohmann/json.hpp>

#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <optional>
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
    } else {
      throw std::invalid_argument("unknown argument: " + std::string(argument));
    }
  }
  if (options.redis_host.empty() || !isLowerUuid(options.run_id)) {
    throw std::invalid_argument(
        "Redis host and a lowercase run UUID are required");
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
                              {"bootstrap_manifest_validated", false},
                              {"end_fence_seen", false},
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

[[nodiscard]] bool supportedManifest(const RunManifest &manifest) {
  return manifest.application_table_allowlist ==
             std::vector<std::string>{"benchbase.usertable"} &&
         manifest.transaction_marker_table == "benchbase.session_txn" &&
         manifest.control_table_allowlist ==
             std::vector<std::string>{"benchbase.checker_run_fence"};
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
  if (!supportedManifest(manifest)) {
    throw MonitorError(
        blackbox::MonitorErrorCode::UnsupportedTransaction,
        "RunManifest names an unsupported application/control table layout");
  }
  return ManifestEvidence{.manifest = std::move(manifest),
                          .stream_id = std::move(first->stream_id)};
}

void publishPending(RunController &controller, const StatusWriter &writer,
                    std::optional<std::string> &terminal_payload) {
  for (auto &payload : controller.takePendingStatuses()) {
    (void)writer.publish(payload);
    const auto status = Json::parse(payload);
    if (status.at("lifecycle_state") == "FINAL") {
      if (terminal_payload.has_value()) {
        throw std::runtime_error(
            "controller emitted more than one FINAL status");
      }
      terminal_payload = std::move(payload);
    }
  }
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

int run(const Options &options) {
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
      std::cerr << "outcome=INVALID code=COMPONENT_RESTARTED message="
                   "monitor ownership claim rejected because the owner key or "
                   "status stream already exists\n";
      return 4;
    }
  } catch (const std::exception &error) {
    std::cerr << "outcome=INVALID code=COMPONENT_RESTARTED message="
              << boundedMessage(error.what()) << '\n';
    return 4;
  }
  const StatusWriter status_writer(status_context.get(), status_stream,
                                   deadline);
  RedisContextPtr reader_context;
  try {
    reader_context = connectRedis(options, evidence_deadline);
  } catch (const std::exception &error) {
    const auto payload = minimalInvalidStatus(
        options, "0-0", "OPERATIONAL", "COMPONENT_RESTARTED", error.what());
    (void)status_writer.publish(payload);
    std::cout << "outcome=INVALID code=COMPONENT_RESTARTED message="
              << error.what() << '\n';
    return 4;
  }

  ManifestEvidence manifest_evidence;
  try {
    manifest_evidence =
        readManifest(reader_context.get(), options, evidence_deadline);
  } catch (const MonitorError &error) {
    const auto payload =
        minimalInvalidStatus(options, "0-0", "PROTOCOL",
                             blackbox::toString(error.code()), error.what());
    (void)status_writer.publish(payload);
    std::cout << "outcome=INVALID code=" << blackbox::toString(error.code())
              << " message=" << error.what() << '\n';
    return 4;
  } catch (const RedisReaderError &error) {
    const auto failure = classifyReaderFailure(error);
    const auto payload = minimalInvalidStatus(
        options, "0-0", blackbox::toString(failure.category), failure.code,
        failure.message);
    (void)status_writer.publish(payload);
    std::cout << "outcome=INVALID code=" << failure.code
              << " message=" << failure.message << '\n';
    return 4;
  }

  const auto &manifest = manifest_evidence.manifest;
  const std::string client_stream = "blackbox:client:" + options.run_id;
  std::optional<DebeziumTxnAssembler> assembler_storage;
  std::optional<RunController> controller_storage;
  std::unique_ptr<BlockingRedisReader> reader_storage;
  try {
    assembler_storage.emplace(DebeziumAssemblerConfig{
        .run_id = manifest.run_id,
        .backend_source_id = manifest.backend_source_id,
        .debezium_version = "3.6.1.Final",
        .application_tables = manifest.application_table_allowlist,
        .transaction_marker_table = manifest.transaction_marker_table,
        .run_fence_table = manifest.control_table_allowlist.front(),
    });
    controller_storage.emplace(manifest);
    controller_storage->setLastClientStreamId(manifest_evidence.stream_id);
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
    const auto payload = minimalInvalidStatus(
        options, manifest_evidence.stream_id, "PROTOCOL",
        blackbox::toString(error.code()), error.what(),
        static_cast<std::uint64_t>(manifest.expected_session_ids.size()));
    (void)status_writer.publish(payload);
    std::cout << "outcome=INVALID code=" << blackbox::toString(error.code())
              << " message=" << error.what() << '\n';
    return 4;
  } catch (const RedisReaderError &error) {
    const auto failure = classifyReaderFailure(error);
    const auto payload = minimalInvalidStatus(
        options, manifest_evidence.stream_id,
        blackbox::toString(failure.category), failure.code, failure.message,
        static_cast<std::uint64_t>(manifest.expected_session_ids.size()));
    (void)status_writer.publish(payload);
    std::cout << "outcome=INVALID code=" << failure.code
              << " message=" << failure.message << '\n';
    return 4;
  } catch (const std::bad_alloc &) {
    const auto payload = minimalInvalidStatus(
        options, manifest_evidence.stream_id, "OPERATIONAL",
        "RESOURCE_EXHAUSTED", "monitor construction allocation failed",
        static_cast<std::uint64_t>(manifest.expected_session_ids.size()));
    (void)status_writer.publish(payload);
    std::cout << "outcome=INVALID code=RESOURCE_EXHAUSTED message="
                 "monitor construction allocation failed\n";
    return 4;
  } catch (const std::exception &error) {
    const auto payload = minimalInvalidStatus(
        options, manifest_evidence.stream_id, "OPERATIONAL", "INTERNAL_ERROR",
        error.what(),
        static_cast<std::uint64_t>(manifest.expected_session_ids.size()));
    (void)status_writer.publish(payload);
    std::cout << "outcome=INVALID code=INTERNAL_ERROR message=" << error.what()
              << '\n';
    return 4;
  }

  auto &assembler = *assembler_storage;
  auto &controller = *controller_storage;
  auto &reader = *reader_storage;
  std::optional<std::string> terminal_payload;
  publishPending(controller, status_writer, terminal_payload);

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
      publishPending(controller, status_writer, terminal_payload);
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
        publishPending(controller, status_writer, terminal_payload);
      }
    } catch (const RedisReaderError &error) {
      const auto failure = classifyReaderFailure(error);
      controller.noteExternalFailure(failure.category, failure.code,
                                     failure.message);
      controller.forceInvalidFinal(failure.code, failure.message);
      publishPending(controller, status_writer, terminal_payload);
    } catch (const CdcAssemblerError &error) {
      controller.noteExternalFailure(
          DiagnosticCategory::Protocol,
          std::string(blackbox::toString(error.code())), error.what());
      controller.forceInvalidFinal(
          std::string(blackbox::toString(error.code())), error.what());
      publishPending(controller, status_writer, terminal_payload);
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
    publishPending(controller, status_writer, terminal_payload);
  }

  if (!terminal_payload.has_value()) {
    throw std::runtime_error("controller terminated without one FINAL status");
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
    std::cerr << "outcome=INVALID code=REDIS_PUBLICATION_FAILED message="
              << error.what() << '\n';
    return 4;
  } catch (const std::bad_alloc &) {
    std::cerr << "outcome=INVALID code=RESOURCE_EXHAUSTED message="
                 "monitor allocation failed\n";
    return 4;
  } catch (const std::exception &error) {
    std::cerr << "outcome=INVALID code=INTERNAL_ERROR message=" << error.what()
              << '\n';
    return 4;
  }
}
