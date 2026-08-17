#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace blackbox {

using CanonicalBytes = std::vector<std::byte>;
using NullableYcsbField = std::optional<std::string>;

class CanonicalEncodingError final : public std::runtime_error {
public:
  using std::runtime_error::runtime_error;
};

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
