#pragma once

#include "blackbox/canonical_row_codec.h"

#include <nlohmann/json_fwd.hpp>

#include <string_view>

namespace blackbox {

class CanonicalSmallBankCodec final {
public:
  CanonicalSmallBankCodec() = delete;

  [[nodiscard]] static CanonicalRowImage
  encodeRow(std::string_view qualified_table, const nlohmann::json &row);
  [[nodiscard]] static CanonicalBytes
  encodeKey(std::string_view qualified_table, const nlohmann::json &key);
  [[nodiscard]] static const CanonicalRowCodec *
  tableCodec(std::string_view qualified_table) noexcept;
};

} // namespace blackbox
