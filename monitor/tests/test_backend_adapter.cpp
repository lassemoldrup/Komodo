#include "blackbox/backend_adapter.h"

#include "test_support.h"

#include <nlohmann/json.hpp>

#include <array>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <string_view>

#ifndef BLACKBOX_PROTOCOL_BACKEND_DIR
#error "BLACKBOX_PROTOCOL_BACKEND_DIR must name the protocol backend directory"
#endif

namespace {

using blackbox::BackendAdapterCatalog;
using blackbox::BackendAdapterRegistry;
using blackbox::BackendFactValueRule;
using blackbox::MonitorError;
using blackbox::MonitorErrorCode;
using blackbox::ProtocolRecordParser;
using Json = nlohmann::json;

[[nodiscard]] Json loadBackendContract(const std::string_view backend) {
  const auto path = std::filesystem::path(BLACKBOX_PROTOCOL_BACKEND_DIR) /
                    (std::string(backend) + ".json");
  std::ifstream input(path, std::ios::binary);
  if (!input.good()) {
    throw std::runtime_error("cannot open shared backend contract: " +
                             path.string());
  }
  Json result;
  input >> result;
  return result;
}

[[nodiscard]] std::string_view ruleName(const BackendFactValueRule rule) {
  switch (rule) {
  case BackendFactValueRule::Exact:
    return "exact";
  case BackendFactValueRule::Sha256:
    return "sha256";
  case BackendFactValueRule::CanonicalIdentifier:
    return "canonical_identifier";
  case BackendFactValueRule::PositiveInteger:
    return "positive_integer";
  }
  std::abort();
}

void checkSharedContract(const std::string_view backend) {
  const auto contract = loadBackendContract(backend);
  const auto &adapter = BackendAdapterRegistry::require(backend);
  BLACKBOX_CHECK(contract.at("backend").get<std::string>() == adapter.name());
  BLACKBOX_CHECK(contract.at("connector_contract").at("source_version") ==
                 adapter.connectorVersion());

  const auto capabilities = adapter.normalizedCapabilities();
  const auto &expected_capabilities = contract.at("normalized_capabilities");
  BLACKBOX_CHECK(capabilities.connector_startup_schema_evidence ==
                 (expected_capabilities.at(
                      "connector_startup_schema_evidence") == "required"));
  BLACKBOX_CHECK(
      capabilities.full_before_after_images ==
      expected_capabilities.at("full_before_after_images").get<bool>());
  BLACKBOX_CHECK(capabilities.marker_last ==
                 expected_capabilities.at("marker_last").get<bool>());
  BLACKBOX_CHECK(
      capabilities.transaction_framing ==
      expected_capabilities.at("transaction_framing").get<std::string>());
  BLACKBOX_CHECK(capabilities.commit_order ==
                 expected_capabilities.at("commit_order").get<std::string>());

  const auto rules = adapter.attestationFactContract();
  const auto &expected_rules = contract.at("attestation_fact_contract");
  BLACKBOX_CHECK(rules.size() == expected_rules.size());
  for (std::size_t index = 0; index < rules.size(); ++index) {
    BLACKBOX_CHECK(rules[index].name ==
                   expected_rules.at(index).at("name").get<std::string>());
    BLACKBOX_CHECK(
        ruleName(rules[index].value_rule) ==
        expected_rules.at(index).at("value_rule").get<std::string>());
    if (rules[index].value_rule == BackendFactValueRule::Exact) {
      BLACKBOX_CHECK(
          rules[index].expected_value ==
          expected_rules.at(index).at("expected_value").get<std::string>());
    }
  }
}

void checkRegistry() {
  BLACKBOX_CHECK(
      blackbox::toString(MonitorErrorCode::SessionContractMismatch) ==
      "SESSION_CONTRACT_MISMATCH");
  BLACKBOX_CHECK(BackendAdapterRegistry::adapters().size() == 2U);
  BLACKBOX_CHECK(BackendAdapterRegistry::find("mariadb") != nullptr);
  BLACKBOX_CHECK(BackendAdapterRegistry::find("yugabytedb_ysql") != nullptr);
  BLACKBOX_CHECK(BackendAdapterRegistry::find("unknown") == nullptr);
  try {
    (void)BackendAdapterRegistry::require("unknown");
    BLACKBOX_CHECK(false);
  } catch (const MonitorError &error) {
    BLACKBOX_CHECK(error.code() == MonitorErrorCode::ConfigInvalid);
  }
}

void checkCatalogOnlySchemaCapability() {
  auto decoder = BackendAdapterRegistry::require("yugabytedb_ysql")
                     .createCdcStartupSchemaDecoder({});
  BLACKBOX_REQUIRE(decoder != nullptr);
  BLACKBOX_CHECK(decoder->sawRecord());
  BLACKBOX_CHECK(decoder->complete());
  try {
    (void)decoder->accept(Json::object(), Json::object());
    BLACKBOX_CHECK(false);
  } catch (const blackbox::AdapterSchemaError &) {
  }
}

void checkYugabyteSchemaAttestationFailures() {
  const auto backend_directory =
      std::filesystem::path(BLACKBOX_PROTOCOL_BACKEND_DIR);
  const auto example_path = backend_directory.parent_path() / "examples" /
                            "backends" /
                            "yugabytedb-ysql-schema-attestation.json";
  std::ifstream input(example_path, std::ios::binary);
  BLACKBOX_REQUIRE(input.good());
  Json document;
  input >> document;
  const auto parsed =
      ProtocolRecordParser{}.parseSchemaAttestation(document.dump());
  const auto &adapter = BackendAdapterRegistry::require("yugabytedb_ysql");
  const auto &benchmark = blackbox::BenchmarkAdapterRegistry::require("ycsb");
  adapter.validateSchemaAttestation(parsed, benchmark);

  const auto expect = [&](auto invalid, const MonitorErrorCode expected) {
    try {
      adapter.validateSchemaAttestation(invalid, benchmark);
      BLACKBOX_CHECK(false);
    } catch (const MonitorError &error) {
      BLACKBOX_CHECK(error.code() == expected);
    }
  };
  auto missing_fact = parsed;
  missing_fact.backend_facts.pop_back();
  expect(std::move(missing_fact), MonitorErrorCode::BackendFactMismatch);
  auto wrong_fact = parsed;
  wrong_fact.backend_facts[0].canonical_value = "other";
  expect(std::move(wrong_fact), MonitorErrorCode::BackendFactMismatch);
  auto extra_fact = parsed;
  extra_fact.backend_facts.push_back({.name = "extra", .canonical_value = "x"});
  expect(std::move(extra_fact), MonitorErrorCode::BackendFactMismatch);
  for (std::size_t index = 0U; index < parsed.backend_facts.size(); ++index) {
    auto drifted_fact = parsed;
    auto &fact = drifted_fact.backend_facts[index];
    const auto rules = adapter.attestationFactContract();
    const auto rule = std::find_if(
        rules.begin(), rules.end(),
        [&](const auto &candidate) { return candidate.name == fact.name; });
    BLACKBOX_REQUIRE(rule != rules.end());
    switch (rule->value_rule) {
    case blackbox::BackendFactValueRule::Exact:
      fact.canonical_value = "unexpected";
      break;
    case blackbox::BackendFactValueRule::Sha256:
      fact.canonical_value = "not-a-sha256";
      break;
    case blackbox::BackendFactValueRule::CanonicalIdentifier:
      fact.canonical_value = "-invalid";
      break;
    case blackbox::BackendFactValueRule::PositiveInteger:
      fact.canonical_value = "0";
      break;
    }
    expect(std::move(drifted_fact), MonitorErrorCode::BackendFactMismatch);
  }
  auto wrong_table = parsed;
  wrong_table.tables[0].canonical_table_name = "benchbase.unknown_table";
  expect(std::move(wrong_table), MonitorErrorCode::SchemaAttestationMismatch);
  auto wrong_column = parsed;
  wrong_column.tables[0].columns[0].logical_sql_type = "BIGINT";
  expect(std::move(wrong_column), MonitorErrorCode::SchemaAttestationMismatch);
  auto wrong_nullability = parsed;
  wrong_nullability.tables[0].columns[0].nullable =
      !wrong_nullability.tables[0].columns[0].nullable;
  expect(std::move(wrong_nullability),
         MonitorErrorCode::SchemaAttestationMismatch);
  auto wrong_primary_key = parsed;
  wrong_primary_key.tables[0].primary_key_columns[0] = "nonce";
  expect(std::move(wrong_primary_key),
         MonitorErrorCode::SchemaAttestationMismatch);
  auto wrong_column_order = parsed;
  std::swap(wrong_column_order.tables[0].columns[0],
            wrong_column_order.tables[0].columns[1]);
  expect(std::move(wrong_column_order),
         MonitorErrorCode::SchemaAttestationMismatch);
}

class SyntheticBackendAdapter final : public blackbox::BackendAdapter {
public:
  explicit SyntheticBackendAdapter(const blackbox::BackendAdapter &delegate)
      : delegate_(delegate) {}

  [[nodiscard]] std::string_view name() const noexcept override {
    return "synthetic_backend";
  }
  [[nodiscard]] std::string_view connectorVersion() const noexcept override {
    return delegate_.connectorVersion();
  }
  [[nodiscard]] blackbox::BackendServerDefaults
  serverDefaults() const noexcept override {
    return delegate_.serverDefaults();
  }
  [[nodiscard]] blackbox::BackendSessionRequirements
  sessionRequirements() const noexcept override {
    return delegate_.sessionRequirements();
  }
  [[nodiscard]] blackbox::BackendDatabaseDefaults
  databaseDefaults() const noexcept override {
    return delegate_.databaseDefaults();
  }
  [[nodiscard]] blackbox::ApplicationTableRequirements
  applicationTableRequirements() const noexcept override {
    return delegate_.applicationTableRequirements();
  }
  [[nodiscard]] blackbox::BackendNormalizedCapabilities
  normalizedCapabilities() const noexcept override {
    return delegate_.normalizedCapabilities();
  }
  [[nodiscard]] std::span<const blackbox::BackendFactRule>
  attestationFactContract() const noexcept override {
    return delegate_.attestationFactContract();
  }
  [[nodiscard]] std::unique_ptr<blackbox::CdcStartupSchemaDecoder>
  createCdcStartupSchemaDecoder(
      blackbox::CdcStartupSchemaConfig) const override {
    throw std::runtime_error("injected synthetic decoder failure");
  }
  [[nodiscard]] std::unique_ptr<blackbox::BackendCdcDecoder>
  createCdcDecoder(blackbox::BackendCdcDecoderConfig) const override {
    throw std::runtime_error("injected synthetic raw decoder failure");
  }

private:
  const blackbox::BackendAdapter &delegate_;
};

void checkSyntheticCatalogSelectionAndFailurePropagation() {
  const auto &mariadb = BackendAdapterRegistry::require("mariadb");
  SyntheticBackendAdapter synthetic(mariadb);
  const std::array<const blackbox::BackendAdapter *, 2> adapters = {&mariadb,
                                                                    &synthetic};
  const BackendAdapterCatalog catalog(adapters);
  BLACKBOX_CHECK(catalog.require("synthetic_backend").name() ==
                 "synthetic_backend");
  try {
    (void)catalog.require("synthetic_backend")
        .createCdcStartupSchemaDecoder({});
    BLACKBOX_CHECK(false);
  } catch (const std::runtime_error &error) {
    BLACKBOX_CHECK(std::string_view(error.what()).find("injected") !=
                   std::string_view::npos);
  }
  try {
    (void)catalog.require("synthetic_backend").createCdcDecoder({});
    BLACKBOX_CHECK(false);
  } catch (const std::runtime_error &error) {
    BLACKBOX_CHECK(std::string_view(error.what()).find("injected") !=
                   std::string_view::npos);
  }
}

} // namespace

int runBackendAdapterTests() {
  checkSharedContract("mariadb");
  checkSharedContract("yugabytedb_ysql");
  checkRegistry();
  checkCatalogOnlySchemaCapability();
  checkYugabyteSchemaAttestationFailures();
  checkSyntheticCatalogSelectionAndFailurePropagation();
  return 0;
}
