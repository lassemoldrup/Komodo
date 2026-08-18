#pragma once

#include <nlohmann/json.hpp>

#include <cstddef>
#include <limits>
#include <stdexcept>
#include <string_view>

namespace blackbox::detail {

class StrictJsonError : public std::runtime_error {
public:
  using std::runtime_error::runtime_error;
};

class StrictJsonLimitError final : public StrictJsonError {
public:
  using StrictJsonError::StrictJsonError;
};

struct StrictJsonLimits {
  std::size_t max_depth = std::numeric_limits<std::size_t>::max();
  std::size_t max_events = std::numeric_limits<std::size_t>::max();
  std::size_t max_string_bytes = std::numeric_limits<std::size_t>::max();
  std::size_t max_object_members = std::numeric_limits<std::size_t>::max();
};

// nlohmann::json's default DOM parser keeps the last occurrence of a duplicate
// object key. CDC evidence must not admit that ambiguity, so every object is
// checked during parsing before the DOM can overwrite anything.
[[nodiscard]] nlohmann::json parseStrictJson(std::string_view input,
                                             StrictJsonLimits limits = {});

} // namespace blackbox::detail
