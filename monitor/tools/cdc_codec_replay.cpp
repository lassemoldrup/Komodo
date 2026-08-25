#include "blackbox/backend_adapter.h"
#include "blackbox/benchmark_adapter.h"
#include "blackbox/canonical_row_codec.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using blackbox::BackendAdapterRegistry;
using blackbox::BackendCdcDataCollectionCount;
using blackbox::BackendCdcDecoderConfig;
using blackbox::BackendCdcRecord;
using blackbox::BackendCdcRecordKind;
using blackbox::BenchmarkAdapterRegistry;
using blackbox::CanonicalBytes;
using blackbox::CanonicalRowCodec;
using Json = nlohmann::json;

struct Options {
  std::string input;
  std::string backend;
  std::string benchmark;
  std::string backend_source_id;
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
    } else if (argument == "--backend") {
      options.backend = value;
    } else if (argument == "--benchmark") {
      options.benchmark = value;
    } else if (argument == "--backend-source-id") {
      options.backend_source_id = value;
    } else {
      throw std::invalid_argument("unknown argument: " +
                                  std::string(argument));
    }
  }
  if (options.input.empty() || options.backend.empty() ||
      options.benchmark.empty() || options.backend_source_id.empty()) {
    throw std::invalid_argument(
        "--input, --backend, --benchmark, and --backend-source-id are required");
  }
  return options;
}

[[nodiscard]] Json loadCapture(const std::string &path) {
  std::ifstream input(path);
  if (!input) {
    throw std::runtime_error("cannot open capture: " + path);
  }
  Json result = Json::parse(input);
  if (!result.is_array()) {
    throw std::runtime_error("XRANGE capture root is not an array");
  }
  return result;
}

[[nodiscard]] std::pair<Json, Json> parseEntry(const Json &entry) {
  if (!entry.is_array() || entry.size() != 2U || !entry[0].is_string() ||
      !entry[1].is_array() || entry[1].size() != 2U ||
      !entry[1][0].is_string() || !entry[1][1].is_string()) {
    throw std::runtime_error(
        "XRANGE entry is not [stream_id,[Debezium-key,envelope]]");
  }
  return {Json::parse(entry[1][0].get<std::string>()),
          Json::parse(entry[1][1].get<std::string>())};
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

void validateEnd(const BackendCdcRecord &end,
                 const std::vector<BackendCdcRecord> &rows) {
  if (!end.event_count.has_value() || *end.event_count != rows.size()) {
    throw std::runtime_error("END event count differs from decoded rows");
  }
  std::map<std::string, std::uint64_t> expected;
  for (const auto &row : rows) {
    ++expected[row.canonical_table];
  }
  std::map<std::string, std::uint64_t> observed;
  for (const BackendCdcDataCollectionCount &count : end.data_collections) {
    if (!observed.emplace(count.canonical_table, count.event_count).second) {
      throw std::runtime_error("END collection count is duplicated");
    }
  }
  if (observed != expected) {
    throw std::runtime_error("END collection counts differ from decoded rows");
  }
}

int run(const Options &options) {
  const auto &backend = BackendAdapterRegistry::require(options.backend);
  const auto &benchmark = BenchmarkAdapterRegistry::require(options.benchmark);
  std::vector<std::string> application_tables;
  application_tables.reserve(benchmark.tables().size());
  for (const auto &table : benchmark.tables()) {
    application_tables.emplace_back(table.name);
  }
  auto decoder = backend.createCdcDecoder(BackendCdcDecoderConfig{
      .backend_source_id = options.backend_source_id,
      .connector_version = std::string(backend.connectorVersion()),
      .application_tables = application_tables,
      .control_tables = {"benchbase.checker_run_fence",
                         "benchbase.session_txn"},
  });

  bool active = false;
  std::vector<BackendCdcRecord> rows;
  std::map<std::string, std::uint64_t> collection_orders;
  Json frames = Json::array();
  std::size_t application_event_count = 0U;
  for (const auto &entry : loadCapture(options.input)) {
    const auto [key, envelope] = parseEntry(entry);
    BackendCdcRecord record = decoder->decode(key, envelope);
    if (record.kind == BackendCdcRecordKind::Begin) {
      if (active) {
        throw std::runtime_error("nested BEGIN in codec replay");
      }
      active = true;
      rows.clear();
      collection_orders.clear();
      continue;
    }
    if (record.kind == BackendCdcRecordKind::Row) {
      if (!active || record.total_order != rows.size() + 1U ||
          record.data_collection_order !=
              ++collection_orders[record.canonical_table]) {
        throw std::runtime_error("row framing or ordinal differs");
      }
      rows.push_back(std::move(record));
      continue;
    }
    if (record.kind != BackendCdcRecordKind::End || !active) {
      throw std::runtime_error("unexpected record kind in codec replay");
    }
    validateEnd(record, rows);
    if (rows.empty() ||
        rows.back().canonical_table != "benchbase.session_txn") {
      throw std::runtime_error("codec replay transaction marker is not last");
    }
    Json rendered_rows = Json::array();
    std::map<std::string, Json> collapsed_writes;
    for (const auto &row : rows) {
      const auto *codec = benchmark.rowCodec(row.canonical_table);
      if (codec == nullptr) {
        continue;
      }
      const auto event_key = codec->encodeKeyObject(row.key);
      Json final_write;
      if (row.operation == "d") {
        if (!row.after.is_null()) {
          throw std::runtime_error("delete event unexpectedly has a final row");
        }
        final_write = Json{{"state", "absent"},
                           {"key_hex", hex(event_key)}};
      } else if (row.operation == "c" || row.operation == "u") {
        if (!row.after.is_object()) {
          throw std::runtime_error(
              "insert/update event lacks a complete final row");
        }
        const auto after = codec->encodeImage(row.after);
        if (event_key != after.key) {
          throw std::runtime_error(
              "event key differs from the complete final-row identity");
        }
        final_write = Json{{"state", "present"},
                           {"key_hex", hex(after.key)},
                           {"value_hex", hex(after.value)}};
      } else {
        throw std::runtime_error("application event operation is unsupported");
      }
      collapsed_writes.insert_or_assign(
          final_write.at("key_hex").get<std::string>(), final_write);
      Json rendered{{"table", row.canonical_table},
                    {"operation", row.operation},
                    {"event_key_hex", hex(event_key)},
                    {"final_write", std::move(final_write)}};
      rendered_rows.push_back(std::move(rendered));
      ++application_event_count;
    }
    Json normalized_writes = Json::array();
    for (auto &[unused, write] : collapsed_writes) {
      (void)unused;
      normalized_writes.push_back(std::move(write));
    }
    frames.push_back(Json{{"rows", rows.size()},
                          {"application_events", std::move(rendered_rows)},
                          {"normalized_writes", std::move(normalized_writes)}});
    active = false;
  }
  if (active) {
    throw std::runtime_error("codec replay ended inside a transaction");
  }
  std::cout << Json{{"completed_transactions", frames.size()},
                    {"application_events", application_event_count},
                    {"frames", std::move(frames)}}
                   .dump(2)
            << '\n';
  return 0;
}

} // namespace

int main(const int argc, char **argv) {
  try {
    return run(parseOptions(argc, argv));
  } catch (const std::exception &error) {
    std::cerr << "CDC codec replay failed: " << error.what() << '\n';
    return 1;
  }
}
