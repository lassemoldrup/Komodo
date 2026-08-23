#include "blackbox/benchmark_adapter.h"

#include "blackbox/backend_adapter.h"

#include "test_support.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <functional>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#ifndef BLACKBOX_PROTOCOL_ADAPTER_DIR
#error "BLACKBOX_PROTOCOL_ADAPTER_DIR must name the protocol adapter directory"
#endif

#ifndef BLACKBOX_PROTOCOL_GOLDEN_DIR
#error                                                                         \
    "BLACKBOX_PROTOCOL_GOLDEN_DIR must name the protocol golden-vector directory"
#endif

namespace {

using blackbox::AdapterColumnContract;
using blackbox::AdapterSchemaError;
using blackbox::BenchmarkAdapter;
using blackbox::BenchmarkAdapterRegistry;
using blackbox::MonitorError;
using blackbox::MonitorErrorCode;
using blackbox::StartupSchemaColumn;
using blackbox::StartupTableSchema;
using Json = nlohmann::json;

[[nodiscard]] Json loadJson(const std::filesystem::path &path) {
  std::ifstream input(path, std::ios::binary);
  if (!input.good()) {
    throw std::runtime_error("cannot open shared contract: " + path.string());
  }
  Json result;
  input >> result;
  return result;
}

[[nodiscard]] std::vector<std::string> strings(const Json &array) {
  std::vector<std::string> result;
  result.reserve(array.size());
  for (const auto &entry : array) {
    result.push_back(entry.get<std::string>());
  }
  return result;
}

[[nodiscard]] std::vector<std::string>
strings(const std::span<const std::string_view> values) {
  std::vector<std::string> result;
  result.reserve(values.size());
  for (const auto value : values) {
    result.emplace_back(value);
  }
  return result;
}

void expectError(const std::function<void()> &action,
                 const MonitorErrorCode expected) {
  try {
    action();
    BLACKBOX_CHECK(false);
  } catch (const MonitorError &error) {
    BLACKBOX_CHECK(error.code() == expected);
  }
}

void expectSchemaError(const std::function<void()> &action) {
  try {
    action();
    BLACKBOX_CHECK(false);
  } catch (const AdapterSchemaError &) {
  }
}

[[nodiscard]] StartupTableSchema startupSchema(const Json &table) {
  StartupTableSchema result{
      .name = table.at("name").get<std::string>(),
      .ddl = "CREATE TABLE `fixture` () ENGINE=InnoDB DEFAULT "
             "CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
      .default_charset_name = "utf8mb4",
      .primary_key_columns = strings(table.at("key_columns")),
      .columns = {},
  };
  result.columns.reserve(table.at("columns").size());
  std::uint64_t position = 1U;
  for (const auto &column : table.at("columns")) {
    const auto sql_type = column.at("sql_type").get<std::string>();
    const auto open = sql_type.find('(');
    const auto base = sql_type.substr(0U, open);
    std::optional<std::uint64_t> length;
    std::optional<std::uint64_t> scale;
    if (open != std::string::npos) {
      const auto parameters =
          sql_type.substr(open + 1U, sql_type.size() - open - 2U);
      const auto comma = parameters.find(',');
      length = std::stoull(parameters.substr(0U, comma));
      if (comma != std::string::npos) {
        scale = std::stoull(parameters.substr(comma + 1U));
      }
    }
    const bool textual = base == "CHAR" || base == "VARCHAR";
    result.columns.push_back(StartupSchemaColumn{
        .name = column.at("name").get<std::string>(),
        .type_name = base,
        .type_expression = base,
        .length = length,
        .scale = scale,
        .charset_name =
            textual ? std::optional<std::string>{"utf8mb4"} : std::nullopt,
        .position = position,
        .nullable = column.at("nullable").get<bool>(),
        .auto_incremented = column.contains("generated"),
        .generated = column.contains("generated"),
    });
    ++position;
  }
  return result;
}

void checkColumn(const AdapterColumnContract &actual, const Json &expected,
                 const std::set<std::string> &primary_key) {
  BLACKBOX_CHECK(actual.name == expected.at("name").get<std::string>());
  BLACKBOX_CHECK(actual.sql_type == expected.at("sql_type").get<std::string>());
  BLACKBOX_CHECK(actual.nullable == expected.at("nullable").get<bool>());
  BLACKBOX_CHECK(actual.canonical_type ==
                 expected.at("canonical_type").get<std::string>());
  BLACKBOX_CHECK(actual.key == primary_key.contains(std::string(actual.name)));
  if (expected.contains("generated")) {
    BLACKBOX_REQUIRE(actual.generated.has_value());
    BLACKBOX_CHECK(*actual.generated ==
                   expected.at("generated").get<std::string>());
  } else {
    BLACKBOX_CHECK(!actual.generated.has_value());
  }
}

void checkSharedAdapterContract(const std::string_view benchmark) {
  const auto contract =
      loadJson(std::filesystem::path(BLACKBOX_PROTOCOL_ADAPTER_DIR) /
               (std::string(benchmark) + ".json"));
  const auto &adapter = BenchmarkAdapterRegistry::require(benchmark);

  BLACKBOX_CHECK(adapter.name() == contract.at("benchmark").get<std::string>());
  BLACKBOX_CHECK(strings(adapter.transactionTypes()) ==
                 strings(contract.at("transaction_types")));
  BLACKBOX_CHECK(strings(adapter.acceptedIsolations()) ==
                 strings(contract.at("declared_isolations")));
  const auto capabilities = adapter.runtimeCapabilities();
  BLACKBOX_CHECK(
      capabilities.client_trace ==
      contract.at("runtime_capabilities").at("client_trace").get<bool>());
  BLACKBOX_CHECK(
      capabilities.cdc_normalization ==
      contract.at("runtime_capabilities").at("cdc_normalization").get<bool>());
  BLACKBOX_CHECK(
      capabilities.bootstrap_scan ==
      contract.at("runtime_capabilities").at("bootstrap_scan").get<bool>());

  const auto expected_tables = strings(contract.at("application_tables"));
  std::vector<std::string> actual_tables;
  for (const auto &table : adapter.tables()) {
    actual_tables.emplace_back(table.name);
  }
  BLACKBOX_CHECK(actual_tables == expected_tables);
  BLACKBOX_REQUIRE(adapter.tables().size() == contract.at("tables").size());
  for (std::size_t index = 0; index < adapter.tables().size(); ++index) {
    const auto &actual = adapter.tables()[index];
    const auto &expected = contract.at("tables")[index];
    BLACKBOX_CHECK(actual.name == expected.at("name").get<std::string>());
    BLACKBOX_CHECK(strings(actual.primary_key) ==
                   strings(expected.at("key_columns")));
    BLACKBOX_CHECK(strings(actual.value_columns) ==
                   strings(expected.at("value_columns")));
    BLACKBOX_REQUIRE(actual.columns.size() == expected.at("columns").size());
    const auto primary_key_values = strings(expected.at("key_columns"));
    const std::set<std::string> primary_key(primary_key_values.begin(),
                                            primary_key_values.end());
    for (std::size_t column = 0; column < actual.columns.size(); ++column) {
      checkColumn(actual.columns[column], expected.at("columns")[column],
                  primary_key);
    }
  }

  BLACKBOX_REQUIRE(adapter.resultSets().size() ==
                   contract.at("result_sets").size());
  for (std::size_t index = 0; index < adapter.resultSets().size(); ++index) {
    const auto &actual = adapter.resultSets()[index];
    const auto &expected = contract.at("result_sets")[index];
    BLACKBOX_CHECK(actual.transaction_type ==
                   expected.at("transaction_type").get<std::string>());
    BLACKBOX_CHECK(actual.procedure ==
                   expected.at("procedure").get<std::string>());
    BLACKBOX_CHECK(actual.statement_id ==
                   expected.at("statement_id").get<std::string>());
    BLACKBOX_REQUIRE(actual.table.has_value() == expected.contains("table"));
    if (actual.table.has_value()) {
      BLACKBOX_CHECK(*actual.table == expected.at("table").get<std::string>());
    }
    BLACKBOX_CHECK(blackbox::toString(actual.classification) ==
                   expected.at("classification").get<std::string>());
    BLACKBOX_CHECK(blackbox::toString(actual.result_shape) ==
                   expected.at("result_shape").get<std::string>());
    BLACKBOX_CHECK(actual.empty_result ==
                   expected.at("empty_result").get<std::string>());
    BLACKBOX_CHECK(strings(actual.semantics_ignored) ==
                   strings(expected.at("semantics_ignored")));
    BLACKBOX_REQUIRE(actual.omission_reason.has_value() ==
                     expected.contains("omission_reason"));
    if (actual.omission_reason.has_value()) {
      BLACKBOX_CHECK(*actual.omission_reason ==
                     expected.at("omission_reason").get<std::string>());
    }
    if (expected.at("trace_statement_id").is_null()) {
      BLACKBOX_CHECK(!actual.trace_statement_id.has_value());
    } else {
      BLACKBOX_REQUIRE(actual.trace_statement_id.has_value());
      BLACKBOX_CHECK(*actual.trace_statement_id ==
                     expected.at("trace_statement_id").get<std::string>());
    }
    BLACKBOX_CHECK(actual.trace_projection ==
                   expected.at("trace_projection").get<std::string>());
  }

  for (const auto &cardinality : contract.at("loader").at("cardinalities")) {
    BLACKBOX_CHECK(adapter.table(cardinality.at("table").get<std::string>()) !=
                   nullptr);
  }
  for (const auto &relationship : contract.at("loader").at("relationships")) {
    BLACKBOX_CHECK(
        adapter.table(relationship.at("parent_table").get<std::string>()) !=
        nullptr);
    BLACKBOX_CHECK(
        adapter.table(relationship.at("child_table").get<std::string>()) !=
        nullptr);
  }
}

void checkExecutableGoldensUseRegisteredTables() {
  const auto goldens = loadJson(
      std::filesystem::path(BLACKBOX_PROTOCOL_GOLDEN_DIR) / "tpcc_rows.json");
  const auto &adapter = BenchmarkAdapterRegistry::require("tpcc");
  BLACKBOX_CHECK(goldens.at("benchmark").get<std::string>() == adapter.name());
  std::set<std::string> covered_tables;
  for (const auto &vector : goldens.at("vectors")) {
    const auto table = vector.at("table").get<std::string>();
    BLACKBOX_CHECK(adapter.table(table) != nullptr);
    covered_tables.insert(table);
  }
  BLACKBOX_CHECK(covered_tables.size() == adapter.tables().size());
}

void checkRegistryValidationAndCapabilities() {
  BLACKBOX_CHECK(BenchmarkAdapterRegistry::adapters().size() == 3U);
  BLACKBOX_CHECK(BenchmarkAdapterRegistry::find("ycsb") != nullptr);
  BLACKBOX_CHECK(
      BenchmarkAdapterRegistry::find("ycsb_fractured_read_probe") != nullptr);
  BLACKBOX_CHECK(BenchmarkAdapterRegistry::find("tpcc") != nullptr);
  BLACKBOX_CHECK(BenchmarkAdapterRegistry::find("unknown") == nullptr);
  BLACKBOX_CHECK(blackbox::isProtocolName("NewOrder"));
  BLACKBOX_CHECK(blackbox::isProtocolName("a"));
  BLACKBOX_CHECK(blackbox::isProtocolName(std::string(64U, 'A')));
  BLACKBOX_CHECK(!blackbox::isProtocolName(""));
  BLACKBOX_CHECK(!blackbox::isProtocolName("1NewOrder"));
  BLACKBOX_CHECK(!blackbox::isProtocolName("New-Order"));
  BLACKBOX_CHECK(!blackbox::isProtocolName(std::string(65U, 'A')));

  expectError([] { (void)BenchmarkAdapterRegistry::require("unknown"); },
              MonitorErrorCode::ConfigInvalid);

  const auto &tpcc = BenchmarkAdapterRegistry::require("tpcc");
  std::vector<std::string> roster;
  for (const auto &table : tpcc.tables()) {
    roster.emplace_back(table.name);
  }
  tpcc.validateRunConfiguration("REPEATABLE_READ", roster, {});
  std::reverse(roster.begin(), roster.end());
  tpcc.validateRunConfiguration("REPEATABLE_READ", roster, {});
  expectError(
      [&] { tpcc.validateRunConfiguration("READ_COMMITTED", roster, {}); },
      MonitorErrorCode::IsolationMismatch);
  auto wrong_roster = roster;
  wrong_roster.pop_back();
  expectError(
      [&] {
        tpcc.validateRunConfiguration("REPEATABLE_READ", wrong_roster, {});
      },
      MonitorErrorCode::UnexpectedTable);
  tpcc.validateRunConfiguration(
      "REPEATABLE_READ", roster,
      blackbox::AdapterRuntimeCapabilities{.client_trace = false,
                                           .cdc_normalization = true,
                                           .bootstrap_scan = true});
  const auto all_capabilities = blackbox::AdapterRuntimeCapabilities{
      .client_trace = true, .cdc_normalization = true, .bootstrap_scan = true};
  tpcc.validateRunConfiguration("REPEATABLE_READ", roster, all_capabilities);
  BLACKBOX_CHECK(tpcc.runtimeCapabilities().complete());

  const auto &ycsb = BenchmarkAdapterRegistry::require("ycsb");
  BLACKBOX_CHECK(ycsb.runtimeCapabilities().complete());
  std::vector<std::string> ycsb_roster{"benchbase.usertable"};
  ycsb.validateRunConfiguration("REPEATABLE_READ", ycsb_roster,
                                all_capabilities);
  expectError(
      [&] {
        ycsb.validateRunConfiguration("READ_COMMITTED", ycsb_roster,
                                      all_capabilities);
      },
      MonitorErrorCode::IsolationMismatch);
  const auto &fractured =
      BenchmarkAdapterRegistry::require("ycsb_fractured_read_probe");
  fractured.validateRunConfiguration("READ_COMMITTED", ycsb_roster,
                                     all_capabilities);
  fractured.validateRunConfiguration("REPEATABLE_READ", ycsb_roster,
                                     all_capabilities);
}

void checkStartupSchemaContracts() {
  const auto &backend = blackbox::BackendAdapterRegistry::require("mariadb");
  const auto requirements = backend.applicationTableRequirements();
  for (const auto benchmark : {"ycsb", "ycsb_fractured_read_probe", "tpcc"}) {
    const auto contract =
        loadJson(std::filesystem::path(BLACKBOX_PROTOCOL_ADAPTER_DIR) /
                 (std::string(benchmark) + ".json"));
    const auto &adapter = BenchmarkAdapterRegistry::require(benchmark);
    for (const auto &table : contract.at("tables")) {
      adapter.validateStartupTableSchema(startupSchema(table), requirements);
    }
  }

  const auto contract = loadJson(
      std::filesystem::path(BLACKBOX_PROTOCOL_ADAPTER_DIR) / "tpcc.json");
  const auto &tpcc = BenchmarkAdapterRegistry::require("tpcc");
  const auto table_named = [&contract](const std::string_view name) -> Json {
    const auto &tables = contract.at("tables");
    const auto found =
        std::find_if(tables.begin(), tables.end(), [name](const Json &table) {
          return table.at("name").get<std::string>() == name;
        });
    if (found == tables.end()) {
      throw std::runtime_error("shared contract table is missing");
    }
    return *found;
  };

  auto wrong_type = startupSchema(table_named("benchbase.warehouse"));
  wrong_type.columns.front().type_name = "BIGINT";
  wrong_type.columns.front().type_expression = "BIGINT";
  expectSchemaError(
      [&] { tpcc.validateStartupTableSchema(wrong_type, requirements); });

  auto wrong_nullability = startupSchema(table_named("benchbase.customer"));
  wrong_nullability.columns[3].nullable = true;
  expectSchemaError([&] {
    tpcc.validateStartupTableSchema(wrong_nullability, requirements);
  });

  auto wrong_key = startupSchema(table_named("benchbase.stock"));
  std::reverse(wrong_key.primary_key_columns.begin(),
               wrong_key.primary_key_columns.end());
  expectSchemaError(
      [&] { tpcc.validateStartupTableSchema(wrong_key, requirements); });

  auto missing_history_id = startupSchema(table_named("benchbase.history"));
  missing_history_id.columns.erase(missing_history_id.columns.begin());
  expectSchemaError([&] {
    tpcc.validateStartupTableSchema(missing_history_id, requirements);
  });

  auto history_not_generated = startupSchema(table_named("benchbase.history"));
  history_not_generated.columns.front().auto_incremented = false;
  expectSchemaError([&] {
    tpcc.validateStartupTableSchema(history_not_generated, requirements);
  });

  auto history_generated_flag_missing =
      startupSchema(table_named("benchbase.history"));
  history_generated_flag_missing.columns.front().generated = false;
  expectSchemaError([&] {
    tpcc.validateStartupTableSchema(history_generated_flag_missing,
                                    requirements);
  });

  auto history_id_not_first = startupSchema(table_named("benchbase.history"));
  std::swap(history_id_not_first.columns[0], history_id_not_first.columns[1]);
  expectSchemaError([&] {
    tpcc.validateStartupTableSchema(history_id_not_first, requirements);
  });

  auto wrong_order = startupSchema(table_named("benchbase.order_line"));
  std::swap(wrong_order.columns[4], wrong_order.columns[5]);
  expectSchemaError(
      [&] { tpcc.validateStartupTableSchema(wrong_order, requirements); });

  auto wrong_charset = startupSchema(table_named("benchbase.item"));
  wrong_charset.default_charset_name = "latin1";
  expectSchemaError(
      [&] { tpcc.validateStartupTableSchema(wrong_charset, requirements); });

  auto wrong_collation = startupSchema(table_named("benchbase.district"));
  wrong_collation.ddl =
      "CREATE TABLE `fixture` () ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 "
      "COLLATE=utf8mb4_general_ci";
  expectSchemaError(
      [&] { tpcc.validateStartupTableSchema(wrong_collation, requirements); });
}

} // namespace

int runBenchmarkAdapterTests() {
  checkSharedAdapterContract("ycsb");
  checkSharedAdapterContract("ycsb_fractured_read_probe");
  checkSharedAdapterContract("tpcc");
  checkExecutableGoldensUseRegisteredTables();
  checkRegistryValidationAndCapabilities();
  checkStartupSchemaContracts();
  return 0;
}
