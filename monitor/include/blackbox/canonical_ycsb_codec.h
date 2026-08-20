#pragma once

#include "blackbox/canonical_row_codec.h"

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace blackbox {

using NullableYcsbField = std::optional<std::string>;

class CanonicalYcsbCodec final {
public:
  static constexpr std::size_t field_count = 10;

  CanonicalYcsbCodec() = delete;

  [[nodiscard]] static CanonicalBytes
  encodeKey(std::string_view schema, std::string_view table, std::int64_t key);

  [[nodiscard]] static CanonicalBytes
  encodeValue(std::span<const NullableYcsbField> fields);
};

} // namespace blackbox
