#include "blackbox/canonical_tpcc_codec.h"

#include "blackbox/benchmark_adapter.h"
#include "test_support.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <functional>
#include <limits>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>

#ifndef BLACKBOX_PROTOCOL_GOLDEN_DIR
#error                                                                         \
    "BLACKBOX_PROTOCOL_GOLDEN_DIR must name the protocol golden-vector directory"
#endif

#ifndef BLACKBOX_PROTOCOL_FIXTURE_DIR
#error "BLACKBOX_PROTOCOL_FIXTURE_DIR must name the protocol fixture directory"
#endif

namespace {

using blackbox::BenchmarkAdapterRegistry;
using blackbox::CanonicalBytes;
using blackbox::CanonicalEncodingError;
using blackbox::CanonicalTpccCodec;
using Json = nlohmann::json;

[[nodiscard]] Json loadGoldens() {
  const auto path =
      std::filesystem::path(BLACKBOX_PROTOCOL_GOLDEN_DIR) / "tpcc_rows.json";
  std::ifstream input(path, std::ios::binary);
  if (!input.good()) {
    throw std::runtime_error("cannot open TPC-C row goldens: " + path.string());
  }
  Json result;
  input >> result;
  return result;
}

[[nodiscard]] Json loadRealFixture() {
  const auto path = std::filesystem::path(BLACKBOX_PROTOCOL_FIXTURE_DIR) /
                    "mariadb_tpcc_typed_rows.json";
  std::ifstream input(path, std::ios::binary);
  if (!input.good()) {
    throw std::runtime_error("cannot open TPC-C typed CDC fixture: " +
                             path.string());
  }
  Json result;
  input >> result;
  return result;
}

[[nodiscard]] std::string hex(const CanonicalBytes &bytes) {
  constexpr std::string_view digits = "0123456789abcdef";
  std::string result;
  result.reserve(bytes.size() * 2U);
  for (const auto byte : bytes) {
    const auto value = std::to_integer<unsigned int>(byte);
    result.push_back(digits[value >> 4U]);
    result.push_back(digits[value & 0x0fU]);
  }
  return result;
}

[[nodiscard]] std::pair<std::string, std::string>
splitTable(const std::string &qualified) {
  const auto separator = qualified.find('.');
  if (separator == std::string::npos) {
    throw std::runtime_error("golden table is not qualified");
  }
  return {qualified.substr(0U, separator), qualified.substr(separator + 1U)};
}

[[nodiscard]] Json keyObject(const std::string &table, const Json &row) {
  const auto *contract = BenchmarkAdapterRegistry::require("tpcc").table(table);
  if (contract == nullptr) {
    throw std::runtime_error("golden names an unregistered TPC-C table");
  }
  Json result = Json::object();
  for (const auto name : contract->primary_key) {
    result[std::string(name)] = row.at(name);
  }
  return result;
}

void expectError(const std::function<void()> &action,
                 const std::string_view category,
                 const std::string_view label = {}) {
  try {
    action();
    throw std::runtime_error("expected TPC-C codec rejection: " +
                             std::string(label));
  } catch (const CanonicalEncodingError &error) {
    BLACKBOX_CHECK(std::string_view(error.what()).starts_with(category));
  }
}

void checkExecutableGoldens() {
  const auto golden = loadGoldens();
  BLACKBOX_CHECK(golden.at("benchmark") == "tpcc");
  const auto schema = golden.at("canonical_schema").get<std::string>();
  std::map<std::string, Json> rows;

  for (const auto &vector : golden.at("vectors")) {
    const auto name = vector.at("name").get<std::string>();
    const auto qualified = vector.at("table").get<std::string>();
    const auto [vector_schema, table] = splitTable(qualified);
    BLACKBOX_CHECK(vector_schema == schema);
    const auto encoded =
        CanonicalTpccCodec::encodeRow(vector_schema, table, vector.at("row"));
    BLACKBOX_CHECK(hex(encoded.key) ==
                   vector.at("expected_key_hex").get<std::string>());
    BLACKBOX_CHECK(hex(encoded.value) ==
                   vector.at("expected_value_hex").get<std::string>());
    BLACKBOX_CHECK(CanonicalTpccCodec::encodeKey(
                       vector_schema, table,
                       keyObject(qualified, vector.at("row"))) == encoded.key);
    BLACKBOX_CHECK(CanonicalTpccCodec::encodeValue(
                       qualified, vector.at("row")) == encoded.value);
    const auto *codec = CanonicalTpccCodec::tableCodec(qualified);
    BLACKBOX_REQUIRE(codec != nullptr);
    codec->validateKey(encoded.key);
    codec->validatePresentValue(encoded.value);
    rows.emplace(name, vector.at("row"));
  }

  BLACKBOX_CHECK(CanonicalTpccCodec::tableCodec("benchbase.unknown") ==
                 nullptr);

  for (const auto &invalid : golden.at("invalid_vectors")) {
    const auto base_name = invalid.at("base_vector").get<std::string>();
    const auto found = rows.find(base_name);
    BLACKBOX_REQUIRE(found != rows.end());
    auto row = found->second;
    const auto &mutations = invalid.at("mutations");
    for (const auto &[name, value] : mutations.at("set").items()) {
      row[name] = value;
    }
    for (const auto &name : mutations.at("remove")) {
      row.erase(name.get<std::string>());
    }
    for (const auto &[name, value] : mutations.at("add").items()) {
      row[name] = value;
    }
    const auto qualified = invalid.at("table").get<std::string>();
    const auto [invalid_schema, table] = splitTable(qualified);
    const auto category = invalid.at("expected_error").get<std::string>();
    expectError(
        [&] {
          (void)CanonicalTpccCodec::encodeRow(invalid_schema, table, row);
        },
        category, invalid.at("name").get<std::string>());
  }
}

void checkProductionFloatAndEncodedValidation() {
  const auto golden = loadGoldens();
  const auto found = std::find_if(
      golden.at("vectors").begin(), golden.at("vectors").end(),
      [](const Json &vector) {
        return vector.at("name") == "customer_fraction_utf8_pre_epoch";
      });
  BLACKBOX_REQUIRE(found != golden.at("vectors").end());
  auto numeric_row = found->at("row");
  numeric_row["c_ytd_payment"] = 0.33333334;
  const auto numeric =
      CanonicalTpccCodec::encodeRow("benchbase", "customer", numeric_row);
  BLACKBOX_CHECK(hex(numeric.value) ==
                 found->at("expected_value_hex").get<std::string>());

  const auto expected_with_float = [&](const std::string_view raw_bits) {
    auto expected = found->at("expected_value_hex").get<std::string>();
    const auto position = expected.find("3eaaaaab");
    if (position == std::string::npos) {
      throw std::runtime_error("float field is absent from the shared golden");
    }
    expected.replace(position, 8U, raw_bits);
    return expected;
  };
  for (const auto raw_bits : {"00000000", "80000000", "00000001", "807fffff"}) {
    auto edge_row = found->at("row");
    edge_row["c_ytd_payment"] = Json{{"raw_bits_hex", raw_bits}};
    const auto edge =
        CanonicalTpccCodec::encodeRow("benchbase", "customer", edge_row);
    BLACKBOX_CHECK(hex(edge.value) == expected_with_float(raw_bits));
  }
  auto negative_zero_row = found->at("row");
  negative_zero_row["c_ytd_payment"] = -0.0;
  const auto negative_zero =
      CanonicalTpccCodec::encodeRow("benchbase", "customer", negative_zero_row);
  BLACKBOX_CHECK(hex(negative_zero.value) == expected_with_float("80000000"));

  auto infinite_row = found->at("row");
  infinite_row["c_ytd_payment"] = std::numeric_limits<double>::infinity();
  expectError(
      [&] {
        (void)CanonicalTpccCodec::encodeRow("benchbase", "customer",
                                            infinite_row);
      },
      "float_non_finite");

  auto uppercase_bits = found->at("row");
  uppercase_bits["c_ytd_payment"] = Json{{"raw_bits_hex", "3EAAAAAB"}};
  expectError(
      [&] {
        (void)CanonicalTpccCodec::encodeRow("benchbase", "customer",
                                            uppercase_bits);
      },
      "float_non_finite");

  const auto encoded = CanonicalTpccCodec::encodeRow(
      "benchbase", "oorder", golden.at("vectors")[10].at("row"));
  auto bad_value = encoded.value;
  bad_value.at(4U) = std::byte{2};
  expectError(
      [&] {
        CanonicalTpccCodec::tableCodec("benchbase.oorder")
            ->validatePresentValue(bad_value);
      },
      "row_shape");
  auto trailing_key = encoded.key;
  trailing_key.push_back(std::byte{0});
  expectError(
      [&] {
        CanonicalTpccCodec::tableCodec("benchbase.oorder")
            ->validateKey(trailing_key);
      },
      "row_shape");
}

void checkStrictKeyObjects() {
  const auto golden = loadGoldens();
  const auto &vector = golden.at("vectors").front();
  const auto qualified = vector.at("table").get<std::string>();
  const auto [schema, table] = splitTable(qualified);
  auto key = keyObject(qualified, vector.at("row"));
  key["unexpected"] = 1;
  expectError([&] { (void)CanonicalTpccCodec::encodeKey(schema, table, key); },
              "row_shape");
  key = Json::object();
  expectError([&] { (void)CanonicalTpccCodec::encodeKey(schema, table, key); },
              "row_shape");
}

void checkRealMariaDbDebeziumFixture() {
  const auto fixture = loadRealFixture();
  BLACKBOX_CHECK(fixture.at("backend") == "mariadb");
  BLACKBOX_CHECK(fixture.at("debezium_version") == "3.6.1.Final");
  BLACKBOX_CHECK(fixture.at("mariadb_image") == "mariadb:12.3");
  BLACKBOX_CHECK(fixture.at("session_contract") ==
                 "protocol/backends/mariadb.json");
  BLACKBOX_REQUIRE(fixture.at("rows").size() == 11U);
  std::set<std::string> tables;
  std::set<std::string> stream_ids;
  for (const auto &captured : fixture.at("rows")) {
    const auto qualified = captured.at("table").get<std::string>();
    const auto [schema, table] = splitTable(qualified);
    const auto encoded =
        CanonicalTpccCodec::encodeRow(schema, table, captured.at("after"));
    BLACKBOX_CHECK(hex(encoded.key) ==
                   captured.at("expected_key_hex").get<std::string>());
    BLACKBOX_CHECK(hex(encoded.value) ==
                   captured.at("expected_value_hex").get<std::string>());
    BLACKBOX_CHECK(CanonicalTpccCodec::encodeKey(
                       schema, table, captured.at("key")) == encoded.key);
    BLACKBOX_CHECK(CanonicalTpccCodec::encodeValue(
                       qualified, captured.at("after")) == encoded.value);
    const auto *codec = CanonicalTpccCodec::tableCodec(qualified);
    BLACKBOX_REQUIRE(codec != nullptr);
    codec->validateKey(encoded.key);
    codec->validatePresentValue(encoded.value);
    tables.insert(qualified);
    BLACKBOX_CHECK(
        stream_ids.insert(captured.at("stream_id").get<std::string>()).second);
  }
  BLACKBOX_CHECK(tables.size() ==
                 BenchmarkAdapterRegistry::require("tpcc").tables().size());
}

} // namespace

int runCanonicalTpccCodecTests() {
  checkExecutableGoldens();
  checkProductionFloatAndEncodedValidation();
  checkStrictKeyObjects();
  checkRealMariaDbDebeziumFixture();
  return 0;
}
