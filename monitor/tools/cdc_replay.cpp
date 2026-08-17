#include "blackbox/cdc_error.h"
#include "blackbox/cdc_types.h"
#include "blackbox/debezium_assembler.h"

#include <nlohmann/json.hpp>

#include <cstddef>
#include <exception>
#include <fstream>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace {

using blackbox::CanonicalBytes;
using blackbox::CdcAssemblerError;
using blackbox::CdcStreamRecord;
using blackbox::CdcTransaction;
using blackbox::DebeziumAssemblerConfig;
using blackbox::DebeziumTxnAssembler;
using Json = nlohmann::json;

struct Options {
  std::string input;
  std::string run_id;
  std::string backend_source_id = "mariadb-main";
};

[[nodiscard]] Options parseOptions(const int argc, char **argv) {
  Options options;
  for (int index = 1; index < argc; ++index) {
    const std::string_view argument(argv[index]);
    if (index + 1 >= argc) {
      throw std::invalid_argument("missing value after " +
                                  std::string(argument));
    }
    const std::string value(argv[++index]);
    if (argument == "--input") {
      options.input = value;
    } else if (argument == "--run-id") {
      options.run_id = value;
    } else if (argument == "--backend-source-id") {
      options.backend_source_id = value;
    } else {
      throw std::invalid_argument("unknown argument: " + std::string(argument));
    }
  }
  if (options.input.empty() || options.run_id.empty()) {
    throw std::invalid_argument("--input and --run-id are required");
  }
  return options;
}

[[nodiscard]] std::string hex(const CanonicalBytes &bytes) {
  constexpr std::string_view digits = "0123456789abcdef";
  std::string result;
  result.reserve(bytes.size() * 2U);
  for (const auto value : bytes) {
    const auto number = std::to_integer<unsigned int>(value);
    result.push_back(digits[number >> 4U]);
    result.push_back(digits[number & 0x0fU]);
  }
  return result;
}

[[nodiscard]] Json renderTransaction(const CdcTransaction &transaction) {
  Json result{
      {"cdc_seq", transaction.cdc_seq},
      {"kind", blackbox::toString(transaction.kind)},
      {"begin_stream_id", transaction.begin_stream_id},
      {"end_stream_id", transaction.end_stream_id},
      {"writes", Json::array()},
  };
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
  for (const auto &write : transaction.writes) {
    Json rendered{{"key_hex", hex(write.key)},
                  {"value_state", blackbox::toString(write.state)}};
    if (write.value.has_value()) {
      rendered["value_hex"] = hex(*write.value);
    }
    result["writes"].push_back(std::move(rendered));
  }
  return result;
}

[[nodiscard]] Json loadCapture(const std::string &path) {
  std::ifstream input(path);
  if (!input) {
    throw std::runtime_error("cannot open capture: " + path);
  }
  Json capture = Json::parse(input);
  if (!capture.is_array()) {
    throw std::runtime_error("XRANGE capture root is not an array");
  }
  return capture;
}

[[nodiscard]] CdcStreamRecord parseRecord(const Json &entry) {
  if (!entry.is_array() || entry.size() != 2U || !entry[0].is_string() ||
      !entry[1].is_array() || entry[1].size() != 2U ||
      !entry[1][0].is_string() || !entry[1][1].is_string()) {
    throw std::runtime_error(
        "XRANGE entry is not [stream_id,[Debezium-key,envelope]]");
  }
  return CdcStreamRecord{.stream_id = entry[0].get<std::string>(),
                         .key_json = entry[1][0].get<std::string>(),
                         .envelope_json = entry[1][1].get<std::string>()};
}

int run(const Options &options) {
  DebeziumTxnAssembler assembler(DebeziumAssemblerConfig{
      .run_id = options.run_id,
      .backend_source_id = options.backend_source_id,
      .debezium_version = "3.6.1.Final",
      .application_tables = {"benchbase.usertable"},
      .transaction_marker_table = "benchbase.session_txn",
      .run_fence_table = "benchbase.checker_run_fence",
  });
  Json transactions = Json::array();
  for (const auto &entry : loadCapture(options.input)) {
    auto completed = assembler.accept(parseRecord(entry));
    if (completed.has_value()) {
      transactions.push_back(renderTransaction(*completed));
    }
  }
  assembler.finish();
  std::cout << Json{{"completed_transactions", assembler.completedCount()},
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
  } catch (const std::exception &error) {
    std::cerr << "cdc replay failed: " << error.what() << '\n';
    return 1;
  }
}
