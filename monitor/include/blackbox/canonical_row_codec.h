#pragma once

#include <cstddef>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace blackbox {

using CanonicalBytes = std::vector<std::byte>;

class CanonicalEncodingError final : public std::runtime_error {
public:
  using std::runtime_error::runtime_error;
};

// A table codec owns validation of the adapter-defined bytes following the
// common schema/table prefix. Encoding JDBC rows and Debezium images is added
// by the corresponding producer/normalizer without changing this boundary.
class CanonicalRowCodec {
public:
  virtual ~CanonicalRowCodec() = default;

  [[nodiscard]] virtual std::string_view tableName() const noexcept = 0;
  virtual void validateKey(std::span<const std::byte> key) const = 0;
  virtual void validatePresentValue(std::span<const std::byte> value) const = 0;
};

} // namespace blackbox
