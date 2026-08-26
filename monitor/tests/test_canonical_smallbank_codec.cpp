#include "blackbox/canonical_smallbank_codec.h"

#include "test_support.h"

#include <nlohmann/json.hpp>

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>

#ifndef BLACKBOX_PROTOCOL_GOLDEN_DIR
#error "BLACKBOX_PROTOCOL_GOLDEN_DIR must name the protocol golden directory"
#endif

namespace {

using blackbox::CanonicalBytes;
using blackbox::CanonicalSmallBankCodec;
using Json = nlohmann::json;

[[nodiscard]] std::string hex(const CanonicalBytes &bytes) {
  constexpr char digits[] = "0123456789abcdef";
  std::string result;
  result.reserve(bytes.size() * 2U);
  for (const auto value : bytes) {
    const auto number = std::to_integer<unsigned int>(value);
    result.push_back(digits[number >> 4U]);
    result.push_back(digits[number & 0x0fU]);
  }
  return result;
}

void checkGoldenRows() {
  const auto path = std::filesystem::path(BLACKBOX_PROTOCOL_GOLDEN_DIR) /
                    "smallbank_rows.json";
  std::ifstream input(path);
  BLACKBOX_REQUIRE(input.good());
  Json document;
  input >> document;
  BLACKBOX_CHECK(document.at("benchmark") == "smallbank");
  for (const auto &vector : document.at("vectors")) {
    const auto table = vector.at("table").get<std::string>();
    const auto encoded =
        CanonicalSmallBankCodec::encodeRow(table, vector.at("row"));
    BLACKBOX_CHECK(hex(encoded.key) ==
                   vector.at("expected_key_hex").get<std::string>());
    BLACKBOX_CHECK(hex(encoded.value) ==
                   vector.at("expected_value_hex").get<std::string>());
    const auto key = CanonicalSmallBankCodec::encodeKey(
        table, Json{{"custid", vector.at("row").at("custid")}});
    BLACKBOX_CHECK(key == encoded.key);
    BLACKBOX_REQUIRE(CanonicalSmallBankCodec::tableCodec(table) != nullptr);
    CanonicalSmallBankCodec::tableCodec(table)->validateKey(encoded.key);
    CanonicalSmallBankCodec::tableCodec(table)->validatePresentValue(
        encoded.value);
  }
}

void checkFailures() {
  try {
    (void)CanonicalSmallBankCodec::encodeRow(
        "benchbase.checking", Json{{"custid", 1}, {"bal", "1.2"}});
    BLACKBOX_CHECK(false);
  } catch (const blackbox::CanonicalEncodingError &) {
  }
  try {
    (void)CanonicalSmallBankCodec::encodeRow(
        "benchbase.accounts", Json{{"custid", "01"}, {"name", "noncanonical"}});
    BLACKBOX_CHECK(false);
  } catch (const blackbox::CanonicalEncodingError &) {
  }
  try {
    (void)CanonicalSmallBankCodec::encodeRow(
        "benchbase.savings",
        Json{{"custid", 1}, {"bal", "1.00"}, {"unexpected", 1}});
    BLACKBOX_CHECK(false);
  } catch (const blackbox::CanonicalEncodingError &) {
  }
}

} // namespace

int runCanonicalSmallBankCodecTests() {
  checkGoldenRows();
  checkFailures();
  return 0;
}
