#include "blackbox/backend_adapter.h"
#include "blackbox/benchmark_adapter.h"
#include "blackbox/cdc_error.h"
#include "blackbox/debezium_assembler.h"
#include "blackbox/monitor_error.h"
#include "blackbox/protocol_records.h"
#include "blackbox/run_controller.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <fstream>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace {

using blackbox::CdcAssemblerError;
using blackbox::CdcStreamRecord;
using blackbox::CdcTransaction;
using blackbox::DebeziumAssemblerConfig;
using blackbox::DebeziumTxnAssembler;
using blackbox::DiagnosticCategory;
using blackbox::MonitorError;
using blackbox::ProtocolBackendFact;
using blackbox::ProtocolRecordParser;
using blackbox::RunController;
using Json = nlohmann::json;

struct Options {
  std::string client_input;
  std::string cdc_input;
  std::vector<ProtocolBackendFact> expected_backend_facts;
};

struct StreamId {
  std::uint64_t milliseconds = 0U;
  std::uint64_t sequence = 0U;

  auto operator<=>(const StreamId &) const = default;
};

enum class EvidenceKind { Cdc, Client };

struct EvidenceRecord {
  EvidenceKind kind = EvidenceKind::Cdc;
  StreamId parsed_stream_id;
  std::string stream_id;
  std::string first;
  std::string second;
};

[[nodiscard]] std::uint64_t decimal(std::string_view value,
                                    const std::string_view label) {
  if (value.empty() || (value.size() > 1U && value.front() == '0')) {
    throw std::invalid_argument(std::string(label) + " is not canonical");
  }
  std::uint64_t result = 0U;
  const auto parsed =
      std::from_chars(value.data(), value.data() + value.size(), result);
  if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size()) {
    throw std::invalid_argument(std::string(label) + " is not U64 decimal");
  }
  return result;
}

[[nodiscard]] StreamId parseStreamId(const std::string_view value) {
  const auto separator = value.find('-');
  if (separator == std::string_view::npos ||
      value.find('-', separator + 1U) != std::string_view::npos) {
    throw std::invalid_argument("evidence stream ID is malformed");
  }
  return StreamId{
      .milliseconds = decimal(value.substr(0U, separator), "stream milliseconds"),
      .sequence = decimal(value.substr(separator + 1U), "stream sequence"),
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
    if (argument == "--client-input") {
      options.client_input = value;
    } else if (argument == "--cdc-input") {
      options.cdc_input = value;
    } else if (argument == "--expected-backend-fact") {
      const auto separator = value.find('=');
      if (separator == std::string::npos || separator == 0U ||
          separator + 1U >= value.size()) {
        throw std::invalid_argument(
            "--expected-backend-fact must be name=value");
      }
      options.expected_backend_facts.push_back(ProtocolBackendFact{
          .name = value.substr(0U, separator),
          .canonical_value = value.substr(separator + 1U),
      });
    } else {
      throw std::invalid_argument("unknown argument: " +
                                  std::string(argument));
    }
  }
  if (options.client_input.empty() || options.cdc_input.empty()) {
    throw std::invalid_argument("--client-input and --cdc-input are required");
  }
  return options;
}

[[nodiscard]] Json loadCapture(const std::string &path) {
  std::ifstream input(path);
  if (!input) {
    throw std::runtime_error("cannot open evidence capture: " + path);
  }
  auto capture = Json::parse(input);
  if (!capture.is_array() || capture.empty()) {
    throw std::runtime_error("evidence capture is not a nonempty array");
  }
  return capture;
}

[[nodiscard]] EvidenceRecord parseEntry(const Json &entry,
                                        const EvidenceKind kind) {
  if (!entry.is_array() || entry.size() != 2U || !entry[0].is_string() ||
      !entry[1].is_array() || entry[1].size() != 2U ||
      !entry[1][0].is_string() || !entry[1][1].is_string()) {
    throw std::runtime_error("evidence entry has a malformed XRANGE shape");
  }
  auto stream_id = entry[0].get<std::string>();
  return EvidenceRecord{
      .kind = kind,
      .parsed_stream_id = parseStreamId(stream_id),
      .stream_id = std::move(stream_id),
      .first = entry[1][0].get<std::string>(),
      .second = entry[1][1].get<std::string>(),
  };
}

[[nodiscard]] Json renderTransaction(const CdcTransaction &transaction) {
  Json writes = Json::array();
  constexpr std::string_view digits = "0123456789abcdef";
  const auto hex = [&](const auto &bytes) {
    std::string result;
    result.reserve(bytes.size() * 2U);
    for (const auto value : bytes) {
      const auto number = std::to_integer<unsigned int>(value);
      result.push_back(digits[number >> 4U]);
      result.push_back(digits[number & 0x0fU]);
    }
    return result;
  };
  for (const auto &write : transaction.writes) {
    Json value{{"key_hex", hex(write.key)},
               {"value_state", blackbox::toString(write.state)}};
    if (write.value.has_value()) {
      value["value_hex"] = hex(*write.value);
    }
    writes.push_back(std::move(value));
  }
  Json result{{"cdc_seq", transaction.cdc_seq},
              {"kind", blackbox::toString(transaction.kind)},
              {"begin_stream_id", transaction.begin_stream_id},
              {"end_stream_id", transaction.end_stream_id},
              {"writes", std::move(writes)}};
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
  const auto client_capture = loadCapture(options.client_input);
  const auto cdc_capture = loadCapture(options.cdc_input);
  auto manifest_entry = parseEntry(client_capture.front(), EvidenceKind::Client);
  if (manifest_entry.first != "payload") {
    throw std::runtime_error("first client record does not use the payload field");
  }
  const auto manifest =
      ProtocolRecordParser{}.parseRunManifest(manifest_entry.second);
  const auto &backend =
      blackbox::BackendAdapterRegistry::require(manifest.backend);
  const auto &benchmark =
      blackbox::BenchmarkAdapterRegistry::require(manifest.benchmark);
  blackbox::RunControllerConfig controller_config;
  controller_config.expected_backend_facts = options.expected_backend_facts;
  RunController controller(manifest, std::move(controller_config));
  controller.setLastClientStreamId(manifest_entry.stream_id);
  DebeziumTxnAssembler assembler(DebeziumAssemblerConfig{
      .run_id = manifest.run_id,
      .backend_source_id = manifest.backend_source_id,
      .debezium_version = std::string(backend.connectorVersion()),
      .backend_adapter = &backend,
      .benchmark_adapter = &benchmark,
      .application_tables = manifest.application_table_allowlist,
      .transaction_marker_table = manifest.transaction_marker_table,
      .run_fence_table = manifest.control_table_allowlist.front(),
  });

  std::vector<EvidenceRecord> evidence;
  evidence.reserve(client_capture.size() + cdc_capture.size() - 1U);
  for (std::size_t index = 1U; index < client_capture.size(); ++index) {
    auto record = parseEntry(client_capture[index], EvidenceKind::Client);
    if (record.first != "payload") {
      throw std::runtime_error("client record does not use the payload field");
    }
    evidence.push_back(std::move(record));
  }
  for (const auto &entry : cdc_capture) {
    evidence.push_back(parseEntry(entry, EvidenceKind::Cdc));
  }
  std::stable_sort(evidence.begin(), evidence.end(),
                   [](const auto &left, const auto &right) {
                     if (left.parsed_stream_id != right.parsed_stream_id) {
                       return left.parsed_stream_id < right.parsed_stream_id;
                     }
                     return left.kind == EvidenceKind::Cdc &&
                            right.kind == EvidenceKind::Client;
                   });

  Json transactions = Json::array();
  Json statuses = Json::array();
  const auto collectStatuses = [&]() {
    for (auto &payload : controller.takePendingStatuses()) {
      statuses.push_back(Json::parse(payload));
    }
  };
  collectStatuses();
  try {
    bool assembler_failed = false;
    for (const auto &record : evidence) {
      if (controller.terminal()) {
        throw std::runtime_error("evidence continues after production FINAL");
      }
      if (record.kind == EvidenceKind::Client) {
        controller.setLastClientStreamId(record.stream_id);
        controller.acceptClientPayload(record.second);
      } else {
        controller.setLastCdcStreamId(record.stream_id);
        try {
          auto completed = assembler.accept(CdcStreamRecord{
              .stream_id = record.stream_id,
              .key_json = record.first,
              .envelope_json = record.second,
          });
          if (completed.has_value()) {
            transactions.push_back(renderTransaction(*completed));
            controller.acceptCdcTransaction(std::move(*completed));
          }
        } catch (const CdcAssemblerError &error) {
          controller.noteExternalFailure(
              DiagnosticCategory::Protocol,
              std::string(blackbox::toString(error.code())), error.what());
          controller.forceInvalidFinal(
              std::string(blackbox::toString(error.code())), error.what());
          assembler_failed = true;
        }
      }
      collectStatuses();
      if (assembler_failed) {
        break;
      }
    }
    if (!assembler_failed) {
      assembler.finish();
    }
    controller.onInputIdle();
    if (!controller.terminal()) {
      controller.forceTimeoutFinal();
    }
    collectStatuses();
  } catch (const MonitorError &error) {
    std::cerr << blackbox::toString(error.code()) << ": " << error.what()
              << '\n';
    return 2;
  }

  const auto final = std::find_if(statuses.begin(), statuses.end(),
                                  [](const auto &status) {
                                    return status.value("lifecycle_state", "") ==
                                           "FINAL";
                                  });
  if (final == statuses.end() ||
      std::count_if(statuses.begin(), statuses.end(), [](const auto &status) {
        return status.value("lifecycle_state", "") == "FINAL";
      }) != 1) {
    throw std::runtime_error("production replay did not emit exactly one FINAL");
  }
  std::cout << Json{{"kind", "production_evidence_replay"},
                    {"run_id", manifest.run_id},
                    {"final_status", *final},
                    {"transactions", std::move(transactions)},
                    {"status_records", std::move(statuses)}}
                   .dump(2)
            << '\n';
  return 0;
}

} // namespace

int main(const int argc, char **argv) {
  try {
    return run(parseOptions(argc, argv));
  } catch (const std::exception &error) {
    std::cerr << "production evidence replay failed: " << error.what() << '\n';
    return 1;
  }
}
