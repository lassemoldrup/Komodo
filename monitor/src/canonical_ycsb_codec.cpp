#include "blackbox/canonical_ycsb_codec.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

namespace blackbox {
namespace {

constexpr std::byte null_field_tag{0};
constexpr std::byte present_field_tag{1};
constexpr std::size_t maximum_field_characters = 100U;

[[nodiscard]] bool isCanonicalIdentifier(const std::string_view value) {
  if (value.empty() || value.size() > 64) {
    return false;
  }

  const auto isLowercaseAscii = [](const char character) {
    return character >= 'a' && character <= 'z';
  };
  const auto isAsciiDigit = [](const char character) {
    return character >= '0' && character <= '9';
  };

  if (!isLowercaseAscii(value.front())) {
    return false;
  }
  for (const char character : value.substr(1)) {
    if (!isLowercaseAscii(character) && !isAsciiDigit(character) &&
        character != '_') {
      return false;
    }
  }
  return true;
}

void requireCanonicalIdentifier(const std::string_view value,
                                const std::string_view label) {
  if (!isCanonicalIdentifier(value)) {
    throw CanonicalEncodingError(std::string(label) +
                                 " is not a canonical lowercase identifier");
  }
}

[[nodiscard]] bool isContinuation(const std::uint8_t byte) {
  return (byte & 0xc0U) == 0x80U;
}

[[nodiscard]] bool isStrictUtf8(const std::string_view value) {
  std::size_t index = 0;
  while (index < value.size()) {
    const auto first = static_cast<std::uint8_t>(value[index]);
    if (first <= 0x7fU) {
      ++index;
      continue;
    }

    if (first >= 0xc2U && first <= 0xdfU) {
      if (index + 1 >= value.size() ||
          !isContinuation(static_cast<std::uint8_t>(value[index + 1]))) {
        return false;
      }
      index += 2;
      continue;
    }

    if (first >= 0xe0U && first <= 0xefU) {
      if (index + 2 >= value.size()) {
        return false;
      }
      const auto second = static_cast<std::uint8_t>(value[index + 1]);
      const auto third = static_cast<std::uint8_t>(value[index + 2]);
      if (!isContinuation(third)) {
        return false;
      }
      if (first == 0xe0U) {
        if (second < 0xa0U || second > 0xbfU) {
          return false;
        }
      } else if (first == 0xedU) {
        if (second < 0x80U || second > 0x9fU) {
          return false;
        }
      } else if (!isContinuation(second)) {
        return false;
      }
      index += 3;
      continue;
    }

    if (first >= 0xf0U && first <= 0xf4U) {
      if (index + 3 >= value.size()) {
        return false;
      }
      const auto second = static_cast<std::uint8_t>(value[index + 1]);
      const auto third = static_cast<std::uint8_t>(value[index + 2]);
      const auto fourth = static_cast<std::uint8_t>(value[index + 3]);
      if (!isContinuation(third) || !isContinuation(fourth)) {
        return false;
      }
      if (first == 0xf0U) {
        if (second < 0x90U || second > 0xbfU) {
          return false;
        }
      } else if (first == 0xf4U) {
        if (second < 0x80U || second > 0x8fU) {
          return false;
        }
      } else if (!isContinuation(second)) {
        return false;
      }
      index += 4;
      continue;
    }

    return false;
  }
  return true;
}

void appendByte(CanonicalBytes &output, const std::uint8_t value) {
  output.push_back(static_cast<std::byte>(value));
}

void appendUint32(CanonicalBytes &output, const std::uint64_t value) {
  if (value > std::numeric_limits<std::uint32_t>::max()) {
    throw CanonicalEncodingError("length-prefixed value exceeds U32");
  }
  const auto narrowed = static_cast<std::uint32_t>(value);
  appendByte(output, static_cast<std::uint8_t>(narrowed >> 24U));
  appendByte(output, static_cast<std::uint8_t>(narrowed >> 16U));
  appendByte(output, static_cast<std::uint8_t>(narrowed >> 8U));
  appendByte(output, static_cast<std::uint8_t>(narrowed));
}

void appendStringBytes(CanonicalBytes &output, const std::string_view value) {
  output.reserve(output.size() + value.size());
  for (const char character : value) {
    output.push_back(
        static_cast<std::byte>(static_cast<unsigned char>(character)));
  }
}

void appendLengthPrefixed(CanonicalBytes &output,
                          const std::string_view value) {
  appendUint32(output, value.size());
  appendStringBytes(output, value);
}

} // namespace

CanonicalBytes CanonicalYcsbCodec::encodeKey(const std::string_view schema,
                                             const std::string_view table,
                                             const std::int64_t key) {
  requireCanonicalIdentifier(schema, "schema");
  requireCanonicalIdentifier(table, "table");
  if (key < std::numeric_limits<std::int32_t>::min() ||
      key > std::numeric_limits<std::int32_t>::max()) {
    throw CanonicalEncodingError("YCSB key is outside signed I32");
  }

  CanonicalBytes output;
  output.reserve(8 + schema.size() + table.size() + 4);
  appendLengthPrefixed(output, schema);
  appendLengthPrefixed(output, table);
  const auto encoded =
      static_cast<std::uint32_t>(static_cast<std::int32_t>(key));
  appendUint32(output, encoded);
  return output;
}

CanonicalBytes CanonicalYcsbCodec::encodeValue(
    const std::span<const NullableYcsbField> fields) {
  if (fields.size() != field_count) {
    throw CanonicalEncodingError("YCSB values require exactly ten fields");
  }

  CanonicalBytes output;
  for (const auto &field : fields) {
    if (!field.has_value()) {
      output.push_back(null_field_tag);
      continue;
    }
    if (!isStrictUtf8(*field)) {
      throw CanonicalEncodingError("YCSB field is not strict UTF-8");
    }
    const auto characters = static_cast<std::size_t>(
        std::count_if(field->begin(), field->end(), [](const char value) {
          return (static_cast<unsigned char>(value) & 0xc0U) != 0x80U;
        }));
    if (characters > maximum_field_characters) {
      throw CanonicalEncodingError("YCSB field exceeds VARCHAR(100)");
    }
    output.push_back(present_field_tag);
    appendLengthPrefixed(output, *field);
  }
  return output;
}

} // namespace blackbox
