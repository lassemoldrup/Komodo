#include "blackbox/cdc_error.h"
#include "blackbox/cdc_types.h"
#include "blackbox/debezium_assembler.h"
#include "blackbox/monitor_error.h"
#include "blackbox/redis_reader.h"
#include "blackbox/transaction_joiner.h"

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
using blackbox::BootstrapPrefix;
using blackbox::BootstrapStateProof;
using blackbox::BootstrapTableCount;
using blackbox::CdcAssemblerError;
using blackbox::CdcStreamRecord;
using blackbox::CdcTransactionKind;
using blackbox::DebeziumAssemblerConfig;
using blackbox::DebeziumTxnAssembler;
using blackbox::MonitorError;
using blackbox::RedisStreamRecord;
using blackbox::RedisStreamSpec;
using blackbox::RunFencePhase;
using blackbox::TransactionJoiner;
using blackbox::TransactionJoinerConfig;
using Json = nlohmann::json;

struct Options {
  std::string redis_host = "host.docker.internal";
  int redis_port = 6379;
  std::string run_id;
  std::string client_cursor;
  std::vector<std::string> expected_session_ids;
  std::chrono::milliseconds overall_timeout{30000};
  std::optional<std::uint64_t> approved_load_cdc_seq;
  std::optional<std::uint64_t> approved_bootstrap_transactions;
  std::optional<std::uint64_t> approved_bootstrap_rows;
  std::string approved_bootstrap_digest;
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

[[nodiscard]] std::uint64_t parseCounter(const std::string_view value,
                                         const std::string_view label) {
  std::uint64_t result = 0;
  const auto [end, error] =
      std::from_chars(value.data(), value.data() + value.size(), result);
  if (error != std::errc{} || end != value.data() + value.size()) {
    throw std::invalid_argument(std::string(label) + " is invalid");
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
    } else if (argument == "--client-cursor") {
      options.client_cursor = value;
    } else if (argument == "--expected-session-id") {
      options.expected_session_ids.push_back(value);
    } else if (argument == "--overall-timeout-ms") {
      options.overall_timeout = parseTimeout(value);
    } else if (argument == "--approved-load-cdc-seq") {
      options.approved_load_cdc_seq = parseCounter(value, argument);
    } else if (argument == "--approved-bootstrap-transactions") {
      options.approved_bootstrap_transactions = parseCounter(value, argument);
    } else if (argument == "--approved-bootstrap-rows") {
      options.approved_bootstrap_rows = parseCounter(value, argument);
    } else if (argument == "--approved-bootstrap-digest") {
      options.approved_bootstrap_digest = value;
    } else {
      throw std::invalid_argument("unknown argument: " + std::string(argument));
    }
  }
  if (options.redis_host.empty() || options.run_id.empty() ||
      options.client_cursor.empty() || options.expected_session_ids.empty() ||
      !options.approved_load_cdc_seq.has_value() ||
      !options.approved_bootstrap_transactions.has_value() ||
      !options.approved_bootstrap_rows.has_value() ||
      options.approved_bootstrap_digest.empty()) {
    throw std::invalid_argument(
        "run, client cursor, expected sessions, and exact bootstrap proof "
        "arguments are required");
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

[[nodiscard]] Json summary(const TransactionJoiner &joiner,
                           const DebeziumTxnAssembler &assembler,
                           const std::size_t client_records,
                           const std::size_t xread_calls) {
  const auto metrics = joiner.metrics();
  Json result{
      {"completed_cdc_transactions", assembler.completedCount()},
      {"accepted_cdc_transactions", metrics.accepted_cdc_transactions},
      {"accepted_client_transactions", metrics.accepted_client_transactions},
      {"joined_workload_transactions", metrics.joined_workload_transactions},
      {"checker_consumed_transactions", metrics.checker.consumed_transactions},
      {"checker_next_commit_seq", metrics.checker.next_commit_seq},
      {"bootstrap_checker_transactions",
       metrics.checker.bootstrap_transactions},
      {"phase_anchor_transactions", metrics.checker.phase_anchor_transactions},
      {"workload_checker_transactions", metrics.checker.workload_transactions},
      {"pending_client_records", metrics.pending_client_records},
      {"pending_cdc_slots", metrics.pending_cdc_slots},
      {"retained_identity_records", metrics.retained_identity_records},
      {"bootstrap_state_rows", metrics.bootstrap_state_rows},
      {"bootstrap_state_bytes", metrics.bootstrap_state_bytes},
      {"client_first_matches", metrics.client_first_matches},
      {"cdc_first_matches", metrics.cdc_first_matches},
      {"end_fence_observed", metrics.end_fence_observed},
      {"checker_observation", blackbox::toString(joiner.checkerObservation())},
      {"head_block_reason", blackbox::toString(joiner.headBlockReason())},
      {"client_records_read", client_records},
      {"xread_calls", xread_calls},
  };
  if (joiner.firstViolation().has_value()) {
    result["first_violation"] = {
        {"run_id", joiner.firstViolation()->run_id},
        {"session_id", joiner.firstViolation()->session_id},
        {"logical_txn_id", joiner.firstViolation()->logical_txn_id},
        {"attempt_id", joiner.firstViolation()->attempt_id},
        {"commit_seq", joiner.firstViolation()->checker_violation.commit_seq},
    };
  }
  return result;
}

int run(const Options &options) {
  auto context = connectRedis(options);
  const std::string client_stream = "blackbox:client:" + options.run_id;
  BlockingRedisReader reader(
      context.get(),
      {RedisStreamSpec{.stream_name = "cdc",
                       .initial_cursor = "0-0",
                       .required_field_name = std::nullopt},
       RedisStreamSpec{.stream_name = client_stream,
                       .initial_cursor = options.client_cursor,
                       .required_field_name = std::string("payload")}},
      1000U, std::chrono::milliseconds(100));
  DebeziumTxnAssembler assembler(DebeziumAssemblerConfig{
      .run_id = options.run_id,
      .backend_source_id = "mariadb-main",
      .debezium_version = "3.6.1.Final",
      .application_tables = {"benchbase.usertable"},
      .transaction_marker_table = "benchbase.session_txn",
      .run_fence_table = "benchbase.checker_run_fence",
  });
  TransactionJoiner joiner(TransactionJoinerConfig{
      .run_id = options.run_id,
      .expected_session_ids = options.expected_session_ids,
      .application_tables = {"benchbase.usertable"},
      .client_parser_limits = {},
      .limits = {},
  });

  std::size_t client_records = 0;
  std::size_t xread_calls = 0;
  bool end_fence_seen = false;
  bool bootstrap_proof_submitted = false;
  const auto deadline =
      std::chrono::steady_clock::now() + options.overall_timeout;
  const auto end_prefix_drained = [&] {
    return end_fence_seen && joiner.headBlockReason() ==
                                 blackbox::HeadBlockReason::EndFenceDeferred;
  };
  while (!end_prefix_drained() && std::chrono::steady_clock::now() < deadline) {
    (void)reader.read_once([&](const RedisStreamRecord &record) {
      if (record.stream_name == client_stream) {
        joiner.acceptClientPayload(record.value);
        ++client_records;
      } else {
        auto completed = assembler.accept(CdcStreamRecord{
            .stream_id = record.stream_id,
            .key_json = record.field_name,
            .envelope_json = record.value,
        });
        if (completed.has_value()) {
          end_fence_seen = completed->kind == CdcTransactionKind::RunControl &&
                           completed->fence_phase == RunFencePhase::EndFence;
          joiner.acceptCdcTransaction(std::move(*completed));
        }
      }
      if (const auto prefix = joiner.pendingBootstrapPrefix();
          prefix.has_value() && !bootstrap_proof_submitted) {
        joiner.validateBootstrapState(BootstrapStateProof{
            .prefix =
                BootstrapPrefix{
                    .run_id = options.run_id,
                    .load_fence_cdc_seq = *options.approved_load_cdc_seq,
                    .bootstrap_transactions =
                        *options.approved_bootstrap_transactions,
                },
            .table_row_counts = {BootstrapTableCount{
                .table = "benchbase.usertable",
                .row_count = *options.approved_bootstrap_rows,
            }},
            .canonical_state_digest = options.approved_bootstrap_digest,
        });
        bootstrap_proof_submitted = true;
      }
      return true;
    });
    ++xread_calls;
  }
  if (!end_fence_seen) {
    throw std::runtime_error("timed out before the CDC END_FENCE transaction");
  }
  if (!end_prefix_drained()) {
    throw std::runtime_error(
        "timed out before the complete pre-END prefix reached the checker");
  }
  assembler.finish();
  std::cout << summary(joiner, assembler, client_records, xread_calls).dump(2)
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
  } catch (const MonitorError &error) {
    std::cerr << blackbox::toString(error.code()) << ": " << error.what()
              << '\n';
    return 2;
  } catch (const blackbox::RedisReaderError &error) {
    if (error.code() == blackbox::RedisReaderErrorCode::CallbackFailed) {
      try {
        std::rethrow_if_nested(error);
      } catch (const CdcAssemblerError &nested) {
        std::cerr << blackbox::toString(nested.code()) << " at "
                  << nested.streamId() << ": " << nested.what() << '\n';
        return 2;
      } catch (const MonitorError &nested) {
        std::cerr << blackbox::toString(nested.code()) << ": " << nested.what()
                  << '\n';
        return 2;
      } catch (...) {
        // Fall through to the reader diagnostic below.
      }
    }
    std::cerr << blackbox::redis_reader_error_name(error.code()) << ": "
              << error.what() << '\n';
    return 1;
  } catch (const std::exception &error) {
    std::cerr << "prefix ingestion failed: " << error.what() << '\n';
    return 1;
  }
}
