#include "blackbox/cdc_error.h"
#include "blackbox/cdc_types.h"
#include "blackbox/debezium_assembler.h"
#include "blackbox/redis_reader.h"

#include <hiredis/hiredis.h>
#include <nlohmann/json.hpp>

#include <charconv>
#include <chrono>
#include <cstddef>
#include <exception>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using blackbox::BlockingRedisReader;
using blackbox::CdcAssemblerError;
using blackbox::CdcStreamRecord;
using blackbox::CdcTransaction;
using blackbox::DebeziumAssemblerConfig;
using blackbox::DebeziumTxnAssembler;
using blackbox::RedisStreamRecord;
using blackbox::RedisStreamSpec;
using blackbox::RunFencePhase;
using Json = nlohmann::json;

struct Options {
  std::string redis_host = "host.docker.internal";
  int redis_port = 6379;
  std::string run_id;
  std::string backend;
  std::string benchmark;
  std::string backend_source_id;
  std::chrono::milliseconds overall_timeout{30000};
};

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
    } else if (argument == "--backend") {
      options.backend = value;
    } else if (argument == "--benchmark") {
      options.benchmark = value;
    } else if (argument == "--backend-source-id") {
      options.backend_source_id = value;
    } else if (argument == "--overall-timeout-ms") {
      options.overall_timeout = parseTimeout(value);
    } else {
      throw std::invalid_argument("unknown argument: " + std::string(argument));
    }
  }
  if (options.redis_host.empty() || options.run_id.empty() ||
      options.backend.empty() || options.benchmark.empty() ||
      options.backend_source_id.empty()) {
    throw std::invalid_argument(
        "--run-id, --backend, --benchmark, --backend-source-id, and a "
        "nonempty Redis host are required");
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

using RedisContextPtr = std::unique_ptr<redisContext, RedisContextDeleter>;

[[nodiscard]] RedisContextPtr connectRedis(const Options &options) {
  const timeval timeout{.tv_sec = 5, .tv_usec = 0};
  RedisContextPtr context(redisConnectWithTimeout(options.redis_host.c_str(),
                                                  options.redis_port, timeout));
  if (!context) {
    throw std::runtime_error("hiredis could not allocate a Redis context");
  }
  if (context->err != 0) {
    throw std::runtime_error(std::string("Redis connection failed: ") +
                             context->errstr);
  }
  return context;
}

[[nodiscard]] Json transactionSummary(const CdcTransaction &transaction) {
  Json result{{"cdc_seq", transaction.cdc_seq},
              {"kind", blackbox::toString(transaction.kind)},
              {"write_count", transaction.writes.size()}};
  if (transaction.fence_phase.has_value()) {
    result["fence_phase"] = blackbox::toString(*transaction.fence_phase);
  }
  if (transaction.correlation_id.has_value()) {
    result["correlation_id"] = {
        {"run_id", transaction.correlation_id->run_id},
        {"session_id", transaction.correlation_id->session_id},
        {"txn_id", transaction.correlation_id->txn_id},
    };
  }
  return result;
}

int run(const Options &options) {
  const auto &backend =
      blackbox::BackendAdapterRegistry::require(options.backend);
  const auto &benchmark =
      blackbox::BenchmarkAdapterRegistry::require(options.benchmark);
  std::vector<std::string> application_tables;
  application_tables.reserve(benchmark.tables().size());
  for (const auto &table : benchmark.tables()) {
    application_tables.emplace_back(table.name);
  }
  auto context = connectRedis(options);
  const std::string client_stream = "blackbox:client:" + options.run_id;
  BlockingRedisReader reader(
      context.get(),
      {RedisStreamSpec{.stream_name = "cdc",
                       .initial_cursor = "0-0",
                       .required_field_name = std::nullopt},
       RedisStreamSpec{.stream_name = client_stream,
                       .initial_cursor = "0-0",
                       .required_field_name = std::string("payload")}},
      1000U, std::chrono::milliseconds(100));
  DebeziumTxnAssembler assembler(DebeziumAssemblerConfig{
      .run_id = options.run_id,
      .backend_source_id = options.backend_source_id,
      .debezium_version = std::string(backend.connectorVersion()),
      .backend_adapter = &backend,
      .benchmark_adapter = &benchmark,
      .application_tables = std::move(application_tables),
      .transaction_marker_table = "benchbase.session_txn",
      .run_fence_table = "benchbase.checker_run_fence",
  });

  Json transactions = Json::array();
  std::size_t client_records = 0;
  std::size_t xread_calls = 0;
  std::size_t nonempty_xread_calls = 0;
  bool end_fence_seen = false;
  const auto deadline =
      std::chrono::steady_clock::now() + options.overall_timeout;
  while (!end_fence_seen && std::chrono::steady_clock::now() < deadline) {
    const auto records_read =
        reader.read_once([&](const RedisStreamRecord &record) {
          if (record.stream_name == client_stream) {
            ++client_records;
            return true;
          }
          auto completed = assembler.accept(CdcStreamRecord{
              .stream_id = record.stream_id,
              .key_json = record.field_name,
              .envelope_json = record.value,
          });
          if (completed.has_value()) {
            transactions.push_back(transactionSummary(*completed));
            end_fence_seen = completed->fence_phase == RunFencePhase::EndFence;
          }
          return true;
        });
    ++xread_calls;
    if (records_read != 0U) {
      ++nonempty_xread_calls;
    }
  }
  if (!end_fence_seen) {
    throw std::runtime_error("timed out before the CDC END_FENCE transaction");
  }
  assembler.finish();
  std::cout << Json{{"completed_transactions", assembler.completedCount()},
                    {"client_records_seen", client_records},
                    {"xread_calls", xread_calls},
                    {"nonempty_xread_calls", nonempty_xread_calls},
                    {"cdc_cursor", std::string(reader.cursor("cdc"))},
                    {"client_cursor",
                     std::string(reader.cursor(client_stream))},
                    {"transactions", std::move(transactions)}}
                   .dump(2)
            << '\n';
  return 0;
}

} // namespace

int main(const int argc, char **argv) {
  try {
    return run(parseOptions(argc, argv));
  } catch (const CdcAssemblerError &error) {
    std::cerr << blackbox::toString(error.code()) << " at " << error.streamId()
              << ": " << error.what() << '\n';
    return 2;
  } catch (const blackbox::RedisReaderError &error) {
    if (error.code() == blackbox::RedisReaderErrorCode::CallbackFailed) {
      try {
        std::rethrow_if_nested(error);
      } catch (const CdcAssemblerError &nested) {
        std::cerr << blackbox::toString(nested.code()) << " at "
                  << nested.streamId() << ": " << nested.what() << '\n';
        return 2;
      } catch (...) {
        // Fall through to the reader diagnostic below. The reader error stays
        // terminal even when a non-assembler callback exception was nested.
      }
    }
    std::cerr << blackbox::redis_reader_error_name(error.code()) << ": "
              << error.what() << '\n';
    return 1;
  } catch (const std::exception &error) {
    std::cerr << "live CDC ingestion failed: " << error.what() << '\n';
    return 1;
  }
}
