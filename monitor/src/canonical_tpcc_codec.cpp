#include "blackbox/canonical_tpcc_codec.h"

#include "blackbox/benchmark_adapter.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <array>
#include <bit>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace blackbox {
namespace {

using Json = nlohmann::json;

[[noreturn]] void fail(const std::string_view category,
                       const std::string_view detail) {
  throw CanonicalEncodingError(std::string(category) + ": " +
                               std::string(detail));
}

[[nodiscard]] bool canonicalIdentifier(const std::string_view value) {
  return !value.empty() && value.size() <= 64U && value.front() >= 'a' &&
         value.front() <= 'z' &&
         std::all_of(value.begin() + 1, value.end(), [](const char character) {
           return (character >= 'a' && character <= 'z') ||
                  (character >= '0' && character <= '9') || character == '_';
         });
}

void appendU32(CanonicalBytes &output, const std::uint32_t value) {
  for (const auto shift : {24U, 16U, 8U, 0U}) {
    output.push_back(static_cast<std::byte>((value >> shift) & 0xffU));
  }
}

void appendU64(CanonicalBytes &output, const std::uint64_t value) {
  for (const auto shift : {56U, 48U, 40U, 32U, 24U, 16U, 8U, 0U}) {
    output.push_back(static_cast<std::byte>((value >> shift) & 0xffU));
  }
}

void appendI32(CanonicalBytes &output, const std::int32_t value) {
  appendU32(output, static_cast<std::uint32_t>(value));
}

void appendI64(CanonicalBytes &output, const std::int64_t value) {
  appendU64(output, static_cast<std::uint64_t>(value));
}

void appendLengthPrefixed(CanonicalBytes &output,
                          const std::string_view value) {
  if (value.size() > std::numeric_limits<std::uint32_t>::max()) {
    fail("text_length", "length-prefixed text exceeds U32");
  }
  appendU32(output, static_cast<std::uint32_t>(value.size()));
  for (const auto byte : value) {
    output.push_back(static_cast<std::byte>(static_cast<unsigned char>(byte)));
  }
}

void appendPrefix(CanonicalBytes &output, const std::string_view schema,
                  const std::string_view table) {
  if (!canonicalIdentifier(schema) || !canonicalIdentifier(table)) {
    fail("row_shape", "schema/table name is not canonical");
  }
  appendLengthPrefixed(output, schema);
  appendLengthPrefixed(output, table);
}

[[nodiscard]] const BenchmarkAdapter &tpccAdapter() {
  return BenchmarkAdapterRegistry::require("tpcc");
}

[[nodiscard]] const AdapterTableContract &
tableContract(const std::string_view qualified_table) {
  const auto *contract = tpccAdapter().table(qualified_table);
  if (contract == nullptr) {
    fail("row_shape", "table is outside the TPC-C adapter roster");
  }
  return *contract;
}

void requireExactRow(const Json &row,
                     const std::span<const AdapterColumnContract> columns) {
  if (!row.is_object() || row.size() != columns.size()) {
    fail("row_shape", "row does not contain the exact table column roster");
  }
  for (const auto &column : columns) {
    if (!row.contains(column.name)) {
      fail("row_shape", "row is missing a declared column");
    }
  }
  for (const auto &[name, unused] : row.items()) {
    (void)unused;
    if (std::find_if(columns.begin(), columns.end(),
                     [&name](const AdapterColumnContract &column) {
                       return column.name == name;
                     }) == columns.end()) {
      fail("row_shape", "row contains an undeclared column");
    }
  }
}

void requireExactKey(const Json &key, const AdapterTableContract &contract) {
  if (!key.is_object() || key.size() != contract.primary_key.size()) {
    fail("row_shape", "key does not contain the exact primary-key roster");
  }
  for (const auto name : contract.primary_key) {
    if (!key.contains(name)) {
      fail("row_shape", "key is missing a primary-key column");
    }
  }
  for (const auto &[name, unused] : key.items()) {
    (void)unused;
    if (std::find(contract.primary_key.begin(), contract.primary_key.end(),
                  name) == contract.primary_key.end()) {
      fail("row_shape", "key contains a non-key column");
    }
  }
}

[[nodiscard]] const AdapterColumnContract &
columnContract(const AdapterTableContract &table, const std::string_view name) {
  const auto found = std::find_if(table.columns.begin(), table.columns.end(),
                                  [name](const AdapterColumnContract &column) {
                                    return column.name == name;
                                  });
  if (found == table.columns.end()) {
    fail("row_shape", "adapter primary/value column has no schema column");
  }
  return *found;
}

[[nodiscard]] std::int64_t jsonInteger(const Json &value,
                                       const std::int64_t minimum,
                                       const std::int64_t maximum) {
  std::int64_t result = 0;
  if (value.is_number_unsigned()) {
    const auto unsigned_value = value.get<std::uint64_t>();
    if (unsigned_value > static_cast<std::uint64_t>(maximum)) {
      fail("integer_range", "unsigned integer exceeds the declared range");
    }
    result = static_cast<std::int64_t>(unsigned_value);
  } else if (value.is_number_integer()) {
    result = value.get<std::int64_t>();
  } else {
    fail("integer_range", "integer column is not a JSON integer");
  }
  if (result < minimum || result > maximum) {
    fail("integer_range", "integer exceeds the declared range");
  }
  return result;
}

[[nodiscard]] std::uint64_t
parseUnsignedDecimal(const std::string_view digits,
                     const std::string_view category) {
  std::uint64_t result = 0;
  const auto parsed =
      std::from_chars(digits.data(), digits.data() + digits.size(), result);
  if (digits.empty() || parsed.ec != std::errc{} ||
      parsed.ptr != digits.data() + digits.size()) {
    fail(category, "numeric text is malformed or out of range");
  }
  return result;
}

struct DecimalType {
  std::uint32_t precision = 0;
  std::uint32_t scale = 0;
};

[[nodiscard]] DecimalType decimalType(const std::string_view sql_type) {
  constexpr std::string_view prefix = "DECIMAL(";
  if (!sql_type.starts_with(prefix) || !sql_type.ends_with(')')) {
    fail("row_shape", "fixed-decimal column has an invalid adapter SQL type");
  }
  const auto body =
      sql_type.substr(prefix.size(), sql_type.size() - prefix.size() - 1U);
  const auto comma = body.find(',');
  if (comma == std::string_view::npos) {
    fail("row_shape", "fixed-decimal adapter SQL type lacks scale");
  }
  const auto precision =
      parseUnsignedDecimal(body.substr(0U, comma), "row_shape");
  const auto scale = parseUnsignedDecimal(body.substr(comma + 1U), "row_shape");
  if (precision == 0U ||
      precision > std::numeric_limits<std::uint32_t>::max() ||
      scale > precision || scale > std::numeric_limits<std::uint32_t>::max()) {
    fail("row_shape", "fixed-decimal adapter SQL type is out of range");
  }
  return DecimalType{.precision = static_cast<std::uint32_t>(precision),
                     .scale = static_cast<std::uint32_t>(scale)};
}

[[nodiscard]] std::int64_t fixedDecimal(const Json &value,
                                        const AdapterColumnContract &column) {
  if (!value.is_string()) {
    fail("decimal_format", "fixed decimal is not a JSON string");
  }
  const auto text = value.get<std::string>();
  const auto type = decimalType(column.sql_type);
  std::size_t cursor = 0;
  const bool negative = !text.empty() && text.front() == '-';
  if (negative) {
    cursor = 1U;
  }
  const auto dot = text.find('.', cursor);
  if (cursor >= text.size() || dot == std::string::npos || dot == cursor ||
      text.find('.', dot + 1U) != std::string::npos) {
    fail("decimal_format", "fixed decimal is not plain signed decimal text");
  }
  const auto integer = std::string_view(text).substr(cursor, dot - cursor);
  const auto fraction = std::string_view(text).substr(dot + 1U);
  const auto all_digits = [](const std::string_view digits) {
    return !digits.empty() &&
           std::all_of(digits.begin(), digits.end(), [](const char character) {
             return character >= '0' && character <= '9';
           });
  };
  if (!all_digits(integer) || !all_digits(fraction) ||
      (integer.size() > 1U && integer.front() == '0')) {
    fail("decimal_format", "fixed decimal has noncanonical digits");
  }
  if (fraction.size() != type.scale) {
    fail("decimal_scale", "fixed decimal does not use its declared scale");
  }
  const auto significant_integer_digits = integer == "0" ? 0U : integer.size();
  if (significant_integer_digits > type.precision - type.scale) {
    fail("decimal_range", "fixed decimal exceeds its declared precision");
  }
  std::string magnitude_text(integer);
  magnitude_text.append(fraction);
  const auto first_nonzero = magnitude_text.find_first_not_of('0');
  if (first_nonzero == std::string::npos) {
    if (negative) {
      fail("decimal_format", "negative zero is not canonical");
    }
    return 0;
  }
  magnitude_text.erase(0U, first_nonzero);
  const auto magnitude = parseUnsignedDecimal(magnitude_text, "decimal_range");
  constexpr auto max_positive =
      static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
  constexpr auto max_negative = max_positive + 1U;
  if ((!negative && magnitude > max_positive) ||
      (negative && magnitude > max_negative)) {
    fail("decimal_range", "fixed-decimal unscaled value exceeds I64");
  }
  if (negative && magnitude == max_negative) {
    return std::numeric_limits<std::int64_t>::min();
  }
  const auto signed_magnitude = static_cast<std::int64_t>(magnitude);
  return negative ? -signed_magnitude : signed_magnitude;
}

[[nodiscard]] std::uint32_t hexU32(const std::string_view value) {
  if (value.size() != 8U ||
      !std::all_of(value.begin(), value.end(), [](const char character) {
        return (character >= '0' && character <= '9') ||
               (character >= 'a' && character <= 'f');
      })) {
    fail("float_non_finite",
         "raw float bits are not eight lowercase hex digits");
  }
  std::uint32_t result = 0;
  const auto parsed =
      std::from_chars(value.data(), value.data() + value.size(), result, 16);
  if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size()) {
    fail("float_non_finite", "raw float bits cannot be parsed");
  }
  return result;
}

[[nodiscard]] std::uint32_t floatBits(const Json &value) {
  std::uint32_t bits = 0;
  if (value.is_object()) {
    if (value.size() != 1U || !value.contains("raw_bits_hex") ||
        !value.at("raw_bits_hex").is_string()) {
      fail("float_non_finite", "float bit object is malformed");
    }
    bits = hexU32(value.at("raw_bits_hex").get<std::string>());
  } else if (value.is_number()) {
    const auto numeric = value.get<double>();
    if (!std::isfinite(numeric)) {
      fail("float_non_finite", "numeric float is not finite");
    }
    const auto narrowed = static_cast<float>(numeric);
    if (!std::isfinite(narrowed)) {
      fail("float_non_finite", "numeric float exceeds binary32");
    }
    bits = std::bit_cast<std::uint32_t>(narrowed);
  } else {
    fail("float_non_finite",
         "float is neither numeric nor an exact bit object");
  }
  if ((bits & 0x7f800000U) == 0x7f800000U) {
    fail("float_non_finite", "binary32 infinity/NaN is outside the contract");
  }
  return bits;
}

[[nodiscard]] unsigned int twoDigits(const std::string_view text,
                                     const std::size_t offset) {
  if (offset + 2U > text.size() || text[offset] < '0' || text[offset] > '9' ||
      text[offset + 1U] < '0' || text[offset + 1U] > '9') {
    fail("timestamp_format", "timestamp contains a non-digit component");
  }
  return static_cast<unsigned int>((text[offset] - '0') * 10 +
                                   (text[offset + 1U] - '0'));
}

[[nodiscard]] std::int64_t timestampSeconds(const Json &value) {
  if (!value.is_string()) {
    fail("timestamp_format", "timestamp is not UTC ISO text");
  }
  const auto text = value.get<std::string>();
  if (text.size() != 20U || text[4] != '-' || text[7] != '-' ||
      text[10] != 'T' || text[13] != ':' || text[16] != ':' ||
      text[19] != 'Z') {
    fail("timestamp_format", "timestamp is not YYYY-MM-DDTHH:MM:SSZ");
  }
  for (std::size_t index = 0; index < 4U; ++index) {
    if (text[index] < '0' || text[index] > '9') {
      fail("timestamp_format", "timestamp year is malformed");
    }
  }
  const int year_value = (text[0] - '0') * 1000 + (text[1] - '0') * 100 +
                         (text[2] - '0') * 10 + (text[3] - '0');
  const auto month_value = twoDigits(text, 5U);
  const auto day_value = twoDigits(text, 8U);
  const auto hour = twoDigits(text, 11U);
  const auto minute = twoDigits(text, 14U);
  const auto second = twoDigits(text, 17U);
  const std::chrono::year_month_day date{std::chrono::year{year_value},
                                         std::chrono::month{month_value},
                                         std::chrono::day{day_value}};
  if (year_value == 0 || !date.ok() || hour > 23U || minute > 59U ||
      second > 59U) {
    fail("timestamp_format", "timestamp calendar/time component is invalid");
  }
  const auto days = std::chrono::sys_days{date}.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::seconds>(days).count() +
         static_cast<std::int64_t>(hour) * 3600 +
         static_cast<std::int64_t>(minute) * 60 +
         static_cast<std::int64_t>(second);
}

struct Utf8Info {
  std::size_t scalar_count = 0;
};

[[nodiscard]] Utf8Info strictUtf8(const std::string_view text) {
  const auto byte = [&text](const std::size_t index) {
    return static_cast<unsigned int>(static_cast<unsigned char>(text[index]));
  };
  const auto continuation = [&byte](const std::size_t index) {
    const auto value = byte(index);
    return value >= 0x80U && value <= 0xbfU;
  };
  Utf8Info result;
  for (std::size_t index = 0; index < text.size();) {
    const auto first = byte(index);
    if (first <= 0x7fU) {
      ++index;
    } else if (first >= 0xc2U && first <= 0xdfU) {
      if (index + 1U >= text.size() || !continuation(index + 1U)) {
        fail("utf8", "text contains malformed UTF-8");
      }
      index += 2U;
    } else if (first >= 0xe0U && first <= 0xefU) {
      if (index + 2U >= text.size() || !continuation(index + 1U) ||
          !continuation(index + 2U) ||
          (first == 0xe0U && byte(index + 1U) < 0xa0U) ||
          (first == 0xedU && byte(index + 1U) > 0x9fU)) {
        fail("utf8", "text contains malformed UTF-8");
      }
      index += 3U;
    } else if (first >= 0xf0U && first <= 0xf4U) {
      if (index + 3U >= text.size() || !continuation(index + 1U) ||
          !continuation(index + 2U) || !continuation(index + 3U) ||
          (first == 0xf0U && byte(index + 1U) < 0x90U) ||
          (first == 0xf4U && byte(index + 1U) > 0x8fU)) {
        fail("utf8", "text contains malformed UTF-8");
      }
      index += 4U;
    } else {
      fail("utf8", "text contains malformed UTF-8");
    }
    ++result.scalar_count;
  }
  return result;
}

[[nodiscard]] std::uint32_t textWidth(const std::string_view sql_type,
                                      const std::string_view prefix) {
  if (!sql_type.starts_with(prefix) || !sql_type.ends_with(')')) {
    fail("row_shape", "text column has invalid adapter SQL type");
  }
  const auto number =
      sql_type.substr(prefix.size(), sql_type.size() - prefix.size() - 1U);
  const auto width = parseUnsignedDecimal(number, "row_shape");
  if (width == 0U || width > std::numeric_limits<std::uint32_t>::max()) {
    fail("row_shape", "text width is outside U32");
  }
  return static_cast<std::uint32_t>(width);
}

void encodeColumn(CanonicalBytes &output, const Json &value,
                  const AdapterColumnContract &column) {
  if (value.is_null()) {
    if (!column.nullable) {
      fail("nullability", "non-null column contains SQL NULL");
    }
    output.push_back(std::byte{0});
    return;
  }
  if (column.nullable) {
    output.push_back(std::byte{1});
  }
  if (column.canonical_type == "i32") {
    appendI32(output, static_cast<std::int32_t>(jsonInteger(
                          value, std::numeric_limits<std::int32_t>::min(),
                          std::numeric_limits<std::int32_t>::max())));
    return;
  }
  if (column.canonical_type == "i64") {
    appendI64(output,
              jsonInteger(value, std::numeric_limits<std::int64_t>::min(),
                          std::numeric_limits<std::int64_t>::max()));
    return;
  }
  if (column.canonical_type == "fixed_decimal") {
    appendI64(output, fixedDecimal(value, column));
    return;
  }
  if (column.canonical_type == "float32") {
    appendU32(output, floatBits(value));
    return;
  }
  if (column.canonical_type == "timestamp_utc") {
    appendI64(output, timestampSeconds(value));
    return;
  }
  if (column.canonical_type == "utf8_varchar" ||
      column.canonical_type == "utf8_char") {
    if (!value.is_string()) {
      fail("utf8", "text column is not a JSON string");
    }
    auto text = value.get<std::string>();
    const auto utf8 = strictUtf8(text);
    const bool fixed = column.canonical_type == "utf8_char";
    const auto width =
        textWidth(column.sql_type, fixed ? std::string_view{"CHAR("}
                                         : std::string_view{"VARCHAR("});
    if (utf8.scalar_count > width) {
      fail("text_length", "text exceeds its declared character width");
    }
    if (fixed) {
      text.append(width - utf8.scalar_count, ' ');
    }
    appendLengthPrefixed(output, text);
    return;
  }
  fail("row_shape", "column has an unsupported canonical type");
}

[[nodiscard]] CanonicalBytes
encodeKeyFromRow(const std::string_view schema, const std::string_view table,
                 const AdapterTableContract &contract, const Json &row) {
  CanonicalBytes output;
  appendPrefix(output, schema, table);
  for (const auto name : contract.primary_key) {
    encodeColumn(output, row.at(name), columnContract(contract, name));
  }
  return output;
}

[[nodiscard]] CanonicalBytes
encodeValueFromRow(const AdapterTableContract &contract, const Json &row) {
  CanonicalBytes output;
  for (const auto name : contract.value_columns) {
    encodeColumn(output, row.at(name), columnContract(contract, name));
  }
  return output;
}

[[nodiscard]] std::uint32_t readU32(const std::span<const std::byte> bytes,
                                    std::size_t &cursor) {
  if (cursor > bytes.size() || bytes.size() - cursor < 4U) {
    fail("row_shape", "canonical bytes have a truncated U32");
  }
  std::uint32_t result = 0;
  for (std::size_t index = 0; index < 4U; ++index) {
    result = static_cast<std::uint32_t>(
        (result << 8U) | std::to_integer<std::uint32_t>(bytes[cursor + index]));
  }
  cursor += 4U;
  return result;
}

[[nodiscard]] std::uint64_t readU64(const std::span<const std::byte> bytes,
                                    std::size_t &cursor) {
  if (cursor > bytes.size() || bytes.size() - cursor < 8U) {
    fail("row_shape", "canonical bytes have a truncated U64");
  }
  std::uint64_t result = 0;
  for (std::size_t index = 0; index < 8U; ++index) {
    result =
        (result << 8U) | std::to_integer<std::uint64_t>(bytes[cursor + index]);
  }
  cursor += 8U;
  return result;
}

[[nodiscard]] std::string_view readText(const std::span<const std::byte> bytes,
                                        std::size_t &cursor) {
  const auto size = static_cast<std::size_t>(readU32(bytes, cursor));
  if (cursor > bytes.size() || size > bytes.size() - cursor) {
    fail("row_shape", "canonical bytes have truncated LP text");
  }
  const auto *data = reinterpret_cast<const char *>(bytes.data() + cursor);
  cursor += size;
  return std::string_view(data, size);
}

void validateEncodedColumn(const std::span<const std::byte> bytes,
                           std::size_t &cursor,
                           const AdapterColumnContract &column) {
  if (column.nullable) {
    if (cursor >= bytes.size()) {
      fail("row_shape", "nullable field tag is truncated");
    }
    const auto tag = std::to_integer<unsigned int>(bytes[cursor++]);
    if (tag == 0U) {
      return;
    }
    if (tag != 1U) {
      fail("row_shape", "nullable field tag is invalid");
    }
  }
  if (column.canonical_type == "i32") {
    (void)readU32(bytes, cursor);
    return;
  }
  if (column.canonical_type == "i64" ||
      column.canonical_type == "timestamp_utc") {
    (void)readU64(bytes, cursor);
    return;
  }
  if (column.canonical_type == "fixed_decimal") {
    const auto raw = readU64(bytes, cursor);
    const auto value = std::bit_cast<std::int64_t>(raw);
    const auto type = decimalType(column.sql_type);
    std::uint64_t magnitude = 0;
    if (value == std::numeric_limits<std::int64_t>::min()) {
      magnitude =
          static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) +
          1U;
    } else {
      magnitude = static_cast<std::uint64_t>(value < 0 ? -value : value);
    }
    std::uint32_t digits = 1U;
    for (auto rest = magnitude; rest >= 10U; rest /= 10U) {
      ++digits;
    }
    if (magnitude != 0U && digits > type.precision) {
      fail("decimal_range", "canonical decimal exceeds declared precision");
    }
    return;
  }
  if (column.canonical_type == "float32") {
    const auto bits = readU32(bytes, cursor);
    if ((bits & 0x7f800000U) == 0x7f800000U) {
      fail("float_non_finite", "canonical binary32 is not finite");
    }
    return;
  }
  if (column.canonical_type == "utf8_varchar" ||
      column.canonical_type == "utf8_char") {
    const auto text = readText(bytes, cursor);
    const auto utf8 = strictUtf8(text);
    const bool fixed = column.canonical_type == "utf8_char";
    const auto width =
        textWidth(column.sql_type, fixed ? std::string_view{"CHAR("}
                                         : std::string_view{"VARCHAR("});
    if ((!fixed && utf8.scalar_count > width) ||
        (fixed && utf8.scalar_count != width)) {
      fail("text_length", "canonical text width is not valid");
    }
    return;
  }
  fail("row_shape", "column has an unsupported canonical type");
}

void validateCanonicalKey(const std::string_view qualified_table,
                          const std::span<const std::byte> bytes) {
  const auto &contract = tableContract(qualified_table);
  const auto separator = qualified_table.find('.');
  if (separator == std::string_view::npos) {
    fail("row_shape", "qualified table is malformed");
  }
  std::size_t cursor = 0;
  const auto schema = readText(bytes, cursor);
  const auto table = readText(bytes, cursor);
  if (schema != qualified_table.substr(0U, separator) ||
      table != qualified_table.substr(separator + 1U)) {
    fail("row_shape", "canonical key prefix names another table");
  }
  for (const auto name : contract.primary_key) {
    validateEncodedColumn(bytes, cursor, columnContract(contract, name));
  }
  if (cursor != bytes.size()) {
    fail("row_shape", "canonical key has trailing bytes");
  }
}

void validateCanonicalValue(const std::string_view qualified_table,
                            const std::span<const std::byte> bytes) {
  const auto &contract = tableContract(qualified_table);
  std::size_t cursor = 0;
  for (const auto name : contract.value_columns) {
    validateEncodedColumn(bytes, cursor, columnContract(contract, name));
  }
  if (cursor != bytes.size()) {
    fail("row_shape", "canonical value has trailing bytes");
  }
}

class TpccTableCodec final : public CanonicalRowCodec {
public:
  constexpr explicit TpccTableCodec(const std::string_view table)
      : table_(table) {}

  [[nodiscard]] std::string_view tableName() const noexcept override {
    return table_;
  }

  void validateKey(const std::span<const std::byte> key) const override {
    validateCanonicalKey(table_, key);
  }

  void
  validatePresentValue(const std::span<const std::byte> value) const override {
    validateCanonicalValue(table_, value);
  }

private:
  std::string_view table_;
};

const std::array table_codecs = {
    TpccTableCodec{"benchbase.warehouse"},
    TpccTableCodec{"benchbase.item"},
    TpccTableCodec{"benchbase.stock"},
    TpccTableCodec{"benchbase.district"},
    TpccTableCodec{"benchbase.customer"},
    TpccTableCodec{"benchbase.history"},
    TpccTableCodec{"benchbase.oorder"},
    TpccTableCodec{"benchbase.new_order"},
    TpccTableCodec{"benchbase.order_line"},
};

} // namespace

CanonicalTpccRow CanonicalTpccCodec::encodeRow(const std::string_view schema,
                                               const std::string_view table,
                                               const Json &row) {
  const auto qualified = std::string(schema) + "." + std::string(table);
  const auto &contract = tableContract(qualified);
  requireExactRow(row, contract.columns);
  return CanonicalTpccRow{
      .key = encodeKeyFromRow(schema, table, contract, row),
      .value = encodeValueFromRow(contract, row),
  };
}

CanonicalBytes CanonicalTpccCodec::encodeKey(const std::string_view schema,
                                             const std::string_view table,
                                             const Json &key) {
  const auto qualified = std::string(schema) + "." + std::string(table);
  const auto &contract = tableContract(qualified);
  requireExactKey(key, contract);
  return encodeKeyFromRow(schema, table, contract, key);
}

CanonicalBytes
CanonicalTpccCodec::encodeValue(const std::string_view qualified_table,
                                const Json &row) {
  const auto &contract = tableContract(qualified_table);
  requireExactRow(row, contract.columns);
  // Validate key fields as well so malformed complete images never produce a
  // canonical value half.
  for (const auto name : contract.primary_key) {
    CanonicalBytes unused;
    encodeColumn(unused, row.at(name), columnContract(contract, name));
  }
  return encodeValueFromRow(contract, row);
}

const CanonicalRowCodec *CanonicalTpccCodec::tableCodec(
    const std::string_view qualified_table) noexcept {
  const auto found =
      std::find_if(table_codecs.begin(), table_codecs.end(),
                   [qualified_table](const TpccTableCodec &codec) {
                     return codec.tableName() == qualified_table;
                   });
  return found == table_codecs.end() ? nullptr : &*found;
}

} // namespace blackbox
