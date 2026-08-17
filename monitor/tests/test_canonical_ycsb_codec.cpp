#include "blackbox/canonical_ycsb_codec.h"
#include "test_support.h"

#include <nlohmann/json.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <functional>
#include <iomanip>
#include <limits>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#ifndef BLACKBOX_PROTOCOL_GOLDEN_DIR
#error                                                                         \
    "BLACKBOX_PROTOCOL_GOLDEN_DIR must name the protocol golden-vector directory"
#endif

namespace {

using blackbox::CanonicalBytes;
using blackbox::CanonicalEncodingError;
using blackbox::CanonicalYcsbCodec;
using blackbox::NullableYcsbField;
using nlohmann::json;

[[nodiscard]] json loadGolden(const std::string_view filename) {
  const std::string path =
      std::string(BLACKBOX_PROTOCOL_GOLDEN_DIR) + '/' + std::string(filename);
  std::ifstream input(path);
  if (!input) {
    throw std::runtime_error("cannot open golden vector: " + path);
  }
  return json::parse(input);
}

[[nodiscard]] std::string toHex(const CanonicalBytes &value) {
  std::ostringstream output;
  output << std::hex << std::setfill('0');
  for (const std::byte byte : value) {
    output << std::setw(2) << std::to_integer<unsigned int>(byte);
  }
  return output.str();
}

[[nodiscard]] bool rejects(const std::function<void()> &operation) {
  try {
    operation();
  } catch (const CanonicalEncodingError &) {
    return true;
  }
  return false;
}

void checkKeyGoldenVectors() {
  const auto golden = loadGolden("ycsb_keys.json");
  const auto schema = golden.at("canonical_schema").get<std::string>();
  const auto table = golden.at("canonical_table").get<std::string>();
  for (const auto &vector : golden.at("vectors")) {
    const auto actual = CanonicalYcsbCodec::encodeKey(
        schema, table, vector.at("key").get<std::int64_t>());
    BLACKBOX_CHECK(toHex(actual) == vector.at("hex").get<std::string>());
  }
}

void checkValueGoldenVectors() {
  const auto golden = loadGolden("ycsb_values.json");
  for (const auto &vector : golden.at("vectors")) {
    std::array<NullableYcsbField, CanonicalYcsbCodec::field_count> fields;
    const auto &encodedFields = vector.at("fields");
    BLACKBOX_CHECK(encodedFields.size() == fields.size());
    for (std::size_t index = 0; index < fields.size(); ++index) {
      if (encodedFields.at(index).is_null()) {
        fields[index] = std::nullopt;
      } else {
        fields[index] = encodedFields.at(index).get<std::string>();
      }
    }
    const auto actual = CanonicalYcsbCodec::encodeValue(fields);
    BLACKBOX_CHECK(actual.size() == vector.at("length").get<std::size_t>());
    BLACKBOX_CHECK(toHex(actual) == vector.at("hex").get<std::string>());
  }
}

void checkIdentifierAndKeyRejection() {
  const auto rejectsKey = [](const std::string_view schema,
                             const std::string_view table,
                             const std::int64_t key) {
    return rejects(
        [&] { (void)CanonicalYcsbCodec::encodeKey(schema, table, key); });
  };

  BLACKBOX_CHECK(rejectsKey("", "usertable", 0));
  BLACKBOX_CHECK(rejectsKey("Benchbase", "usertable", 0));
  BLACKBOX_CHECK(rejectsKey("1benchbase", "usertable", 0));
  BLACKBOX_CHECK(rejectsKey("bench-base", "usertable", 0));
  BLACKBOX_CHECK(rejectsKey(std::string(65, 'a'), "usertable", 0));
  BLACKBOX_CHECK(rejectsKey("benchbase", "user.table", 0));
  BLACKBOX_CHECK(rejectsKey(
      "benchbase", "usertable",
      static_cast<std::int64_t>(std::numeric_limits<std::int32_t>::min()) - 1));
  BLACKBOX_CHECK(rejectsKey(
      "benchbase", "usertable",
      static_cast<std::int64_t>(std::numeric_limits<std::int32_t>::max()) + 1));
}

void checkFieldCountRejection() {
  std::array<NullableYcsbField, CanonicalYcsbCodec::field_count - 1> tooFew;
  std::array<NullableYcsbField, CanonicalYcsbCodec::field_count + 1> tooMany;
  BLACKBOX_CHECK(
      rejects([&] { (void)CanonicalYcsbCodec::encodeValue(tooFew); }));
  BLACKBOX_CHECK(
      rejects([&] { (void)CanonicalYcsbCodec::encodeValue(tooMany); }));
}

void checkUtf8Rejection() {
  const std::vector<std::string> invalid = {
      std::string("\x80", 1),
      std::string("\xc0\x80", 2),
      std::string("\xc1\xbf", 2),
      std::string("\xe0\x80\x80", 3),
      std::string("\xed\xa0\x80", 3),
      std::string("\xf0\x80\x80\x80", 4),
      std::string("\xf4\x90\x80\x80", 4),
      std::string("\xf5\x80\x80\x80", 4),
      std::string("\xe2\x82", 2),
      std::string("\xe2\x28\xa1", 3),
  };

  for (const auto &malformed : invalid) {
    std::array<NullableYcsbField, CanonicalYcsbCodec::field_count> fields{};
    fields[0] = malformed;
    BLACKBOX_CHECK(
        rejects([&] { (void)CanonicalYcsbCodec::encodeValue(fields); }));
  }
}

void checkUtf8BoundaryAcceptance() {
  std::array<NullableYcsbField, CanonicalYcsbCodec::field_count> fields{};
  fields[0] = std::string("\x00\x7f", 2);
  fields[1] = std::string("\xc2\x80", 2);
  fields[2] = std::string("\xdf\xbf", 2);
  fields[3] = std::string("\xe0\xa0\x80", 3);
  fields[4] = std::string("\xed\x9f\xbf", 3);
  fields[5] = std::string("\xee\x80\x80", 3);
  fields[6] = std::string("\xef\xbf\xbf", 3);
  fields[7] = std::string("\xf0\x90\x80\x80", 4);
  fields[8] = std::string("\xf4\x8f\xbf\xbf", 4);
  fields[9] = "";
  BLACKBOX_CHECK(!CanonicalYcsbCodec::encodeValue(fields).empty());
}

} // namespace

int runCanonicalYcsbCodecTests() {
  checkKeyGoldenVectors();
  checkValueGoldenVectors();
  checkIdentifierAndKeyRejection();
  checkFieldCountRejection();
  checkUtf8Rejection();
  checkUtf8BoundaryAcceptance();
  return 0;
}
