#include "blackbox/canonical_smallbank_codec.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <string>
#include <string_view>

namespace blackbox {
namespace {

using Json = nlohmann::json;

[[noreturn]] void fail(const std::string_view message) {
  throw CanonicalEncodingError("smallbank: " + std::string(message));
}

void appendU32(CanonicalBytes &output, const std::uint32_t value) {
  for (const auto shift : {24U, 16U, 8U, 0U}) {
    output.push_back(static_cast<std::byte>((value >> shift) & 0xffU));
  }
}

void appendI64(CanonicalBytes &output, const std::int64_t value) {
  const auto bits = static_cast<std::uint64_t>(value);
  for (const auto shift : {56U, 48U, 40U, 32U, 24U, 16U, 8U, 0U}) {
    output.push_back(static_cast<std::byte>((bits >> shift) & 0xffU));
  }
}

void appendLp(CanonicalBytes &output, const std::string_view value) {
  if (value.size() > std::numeric_limits<std::uint32_t>::max()) {
    fail("length-prefixed value exceeds U32");
  }
  appendU32(output, static_cast<std::uint32_t>(value.size()));
  for (const char character : value) {
    output.push_back(
        static_cast<std::byte>(static_cast<unsigned char>(character)));
  }
}

[[nodiscard]] std::size_t strictUtf8Scalars(const std::string_view text) {
  const auto byte = [&text](const std::size_t index) {
    return static_cast<unsigned int>(static_cast<unsigned char>(text[index]));
  };
  const auto continuation = [&byte](const std::size_t index) {
    const auto value = byte(index);
    return value >= 0x80U && value <= 0xbfU;
  };
  std::size_t scalars = 0;
  for (std::size_t index = 0; index < text.size();) {
    const auto first = byte(index);
    if (first <= 0x7fU) {
      ++index;
    } else if (first >= 0xc2U && first <= 0xdfU) {
      if (index + 1U >= text.size() || !continuation(index + 1U)) {
        fail("text contains malformed UTF-8");
      }
      index += 2U;
    } else if (first >= 0xe0U && first <= 0xefU) {
      if (index + 2U >= text.size() || !continuation(index + 1U) ||
          !continuation(index + 2U) ||
          (first == 0xe0U && byte(index + 1U) < 0xa0U) ||
          (first == 0xedU && byte(index + 1U) > 0x9fU)) {
        fail("text contains malformed UTF-8");
      }
      index += 3U;
    } else if (first >= 0xf0U && first <= 0xf4U) {
      if (index + 3U >= text.size() || !continuation(index + 1U) ||
          !continuation(index + 2U) || !continuation(index + 3U) ||
          (first == 0xf0U && byte(index + 1U) < 0x90U) ||
          (first == 0xf4U && byte(index + 1U) > 0x8fU)) {
        fail("text contains malformed UTF-8");
      }
      index += 4U;
    } else {
      fail("text contains malformed UTF-8");
    }
    ++scalars;
  }
  return scalars;
}

[[nodiscard]] std::int64_t i64(const Json &value) {
  if (value.is_number_integer()) {
    return value.get<std::int64_t>();
  }
  if (value.is_number_unsigned()) {
    const auto number = value.get<std::uint64_t>();
    if (number >
        static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
      fail("custid exceeds signed I64");
    }
    return static_cast<std::int64_t>(number);
  }
  if (!value.is_string()) {
    fail("custid is not an integer or canonical Oracle NUMBER text");
  }
  const auto text = value.get<std::string>();
  std::int64_t result = 0;
  const auto parsed =
      std::from_chars(text.data(), text.data() + text.size(), result);
  if (text.empty() || parsed.ec != std::errc{} ||
      parsed.ptr != text.data() + text.size() ||
      (text.size() > 1U && text.front() == '0') || text == "-0" ||
      (text.starts_with('-') && text.size() > 2U && text[1] == '0')) {
    fail("custid NUMBER text is noncanonical");
  }
  return result;
}

[[nodiscard]] std::int64_t balance(const Json &value) {
  if (!value.is_string()) {
    fail("balance is not canonical decimal text");
  }
  const auto text = value.get<std::string>();
  std::size_t cursor = 0;
  const bool negative = !text.empty() && text.front() == '-';
  if (negative) {
    cursor = 1U;
  }
  const auto dot = text.find('.', cursor);
  if (cursor >= text.size() || dot == cursor || dot == std::string::npos ||
      text.find('.', dot + 1U) != std::string::npos ||
      text.size() - dot - 1U != 2U) {
    fail("balance is not canonical scale-2 decimal text");
  }
  const auto integer = std::string_view(text).substr(cursor, dot - cursor);
  const auto fraction = std::string_view(text).substr(dot + 1U);
  const auto digits = [](const std::string_view value) {
    return !value.empty() &&
           std::all_of(value.begin(), value.end(), [](const char character) {
             return character >= '0' && character <= '9';
           });
  };
  if (!digits(integer) || !digits(fraction) ||
      (integer.size() > 1U && integer.front() == '0')) {
    fail("balance decimal digits are noncanonical");
  }
  const auto significant = integer == "0" ? 0U : integer.size();
  if (significant + 2U > 18U) {
    fail("balance exceeds DECIMAL(18,2)");
  }
  std::string magnitude_text(integer);
  magnitude_text.append(fraction);
  std::uint64_t magnitude = 0;
  const auto parsed =
      std::from_chars(magnitude_text.data(),
                      magnitude_text.data() + magnitude_text.size(), magnitude);
  if (parsed.ec != std::errc{} ||
      parsed.ptr != magnitude_text.data() + magnitude_text.size()) {
    fail("balance unscaled value exceeds U64");
  }
  if (magnitude == 0U && negative) {
    fail("negative zero balance is noncanonical");
  }
  const auto max =
      static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
  if (magnitude > max + static_cast<std::uint64_t>(negative)) {
    fail("balance unscaled value exceeds I64");
  }
  if (negative && magnitude == max + 1U) {
    return std::numeric_limits<std::int64_t>::min();
  }
  const auto signed_value = static_cast<std::int64_t>(magnitude);
  return negative ? -signed_value : signed_value;
}

[[nodiscard]] std::string_view leaf(const std::string_view table) {
  constexpr std::string_view prefix = "benchbase.";
  if (!table.starts_with(prefix)) {
    fail("table is outside the canonical SmallBank schema");
  }
  const auto result = table.substr(prefix.size());
  if (result != "accounts" && result != "checking" && result != "savings") {
    fail("table is outside the SmallBank roster");
  }
  return result;
}

void exactObject(const Json &object,
                 const std::span<const std::string_view> fields) {
  if (!object.is_object() || object.size() != fields.size()) {
    fail("JSON object has the wrong field count");
  }
  for (const auto field : fields) {
    if (!object.contains(field)) {
      fail("JSON object lacks a declared field");
    }
  }
}

[[nodiscard]] CanonicalBytes keyBytes(const std::string_view table,
                                      const Json &key) {
  constexpr std::array fields = {std::string_view{"custid"}};
  exactObject(key, fields);
  CanonicalBytes result;
  appendLp(result, "benchbase");
  appendLp(result, leaf(table));
  appendI64(result, i64(key.at("custid")));
  return result;
}

class SmallBankTableCodec final : public CanonicalRowCodec {
public:
  explicit SmallBankTableCodec(const std::string_view table) : table_(table) {}

  [[nodiscard]] std::string_view tableName() const noexcept override {
    return table_;
  }

  [[nodiscard]] CanonicalBytes encodeKeyObject(const Json &key) const override {
    return keyBytes(table_, key);
  }

  [[nodiscard]] CanonicalRowImage
  encodeImage(const Json &image) const override {
    return CanonicalSmallBankCodec::encodeRow(table_, image);
  }

  void validateKey(const std::span<const std::byte> key) const override {
    const auto expected = 4U + 9U + 4U + leaf(table_).size() + 8U;
    if (key.size() != expected) {
      fail("canonical key length differs");
    }
  }

  void
  validatePresentValue(const std::span<const std::byte> value) const override {
    if (leaf(table_) != "accounts") {
      if (value.size() != 8U) {
        fail("canonical balance length differs");
      }
      return;
    }
    if (value.size() < 4U) {
      fail("canonical account name lacks a length prefix");
    }
    const auto byte = [&value](const std::size_t index) {
      return static_cast<std::uint32_t>(value[index]);
    };
    const auto size =
        (byte(0) << 24U) | (byte(1) << 16U) | (byte(2) << 8U) | byte(3);
    if (size != value.size() - 4U) {
      fail("canonical account name length differs");
    }
    std::string text;
    text.reserve(size);
    for (std::size_t index = 4U; index < value.size(); ++index) {
      text.push_back(static_cast<char>(value[index]));
    }
    if (strictUtf8Scalars(text) > 64U) {
      fail("canonical account name exceeds VARCHAR(64)");
    }
  }

private:
  std::string_view table_;
};

const SmallBankTableCodec accounts_codec("benchbase.accounts");
const SmallBankTableCodec checking_codec("benchbase.checking");
const SmallBankTableCodec savings_codec("benchbase.savings");

} // namespace

CanonicalRowImage
CanonicalSmallBankCodec::encodeRow(const std::string_view table,
                                   const Json &row) {
  const auto table_leaf = leaf(table);
  if (table_leaf == "accounts") {
    constexpr std::array fields = {std::string_view{"custid"},
                                   std::string_view{"name"}};
    exactObject(row, fields);
  } else {
    constexpr std::array fields = {std::string_view{"custid"},
                                   std::string_view{"bal"}};
    exactObject(row, fields);
  }
  CanonicalRowImage result;
  result.key = keyBytes(table, Json{{"custid", row.at("custid")}});
  if (table_leaf == "accounts") {
    if (!row.at("name").is_string()) {
      fail("account name is not text");
    }
    const auto name = row.at("name").get<std::string>();
    if (strictUtf8Scalars(name) > 64U) {
      fail("account name exceeds VARCHAR(64)");
    }
    appendLp(result.value, name);
  } else {
    appendI64(result.value, balance(row.at("bal")));
  }
  return result;
}

CanonicalBytes CanonicalSmallBankCodec::encodeKey(const std::string_view table,
                                                  const Json &key) {
  return keyBytes(table, key);
}

const CanonicalRowCodec *
CanonicalSmallBankCodec::tableCodec(const std::string_view table) noexcept {
  if (table == accounts_codec.tableName()) {
    return &accounts_codec;
  }
  if (table == checking_codec.tableName()) {
    return &checking_codec;
  }
  if (table == savings_codec.tableName()) {
    return &savings_codec;
  }
  return nullptr;
}

} // namespace blackbox
