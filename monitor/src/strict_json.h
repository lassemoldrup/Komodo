#pragma once

#include <nlohmann/json.hpp>

#include <stdexcept>
#include <string_view>

namespace blackbox::detail {

class StrictJsonError final : public std::runtime_error {
public:
  using std::runtime_error::runtime_error;
};

// nlohmann::json's default DOM parser keeps the last occurrence of a duplicate
// object key. CDC evidence must not admit that ambiguity, so every object is
// checked during parsing before the DOM can overwrite anything.
[[nodiscard]] nlohmann::json parseStrictJson(std::string_view input);

} // namespace blackbox::detail
