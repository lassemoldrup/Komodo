#pragma once

#include "blackbox/canonical_row_codec.h"

#include <nlohmann/json_fwd.hpp>

#include <string_view>

namespace blackbox {

struct CanonicalTpccRow {
  CanonicalBytes key;
  CanonicalBytes value;

  bool operator==(const CanonicalTpccRow &) const = default;
};

// Standalone typed codec for the registered TPC-C table contract. It is not
// connected to CDC assembly until the table-normalization dependency lands.
class CanonicalTpccCodec final {
public:
  CanonicalTpccCodec() = delete;

  [[nodiscard]] static CanonicalTpccRow encodeRow(std::string_view schema,
                                                  std::string_view table,
                                                  const nlohmann::json &row);

  // A Debezium key object contains exactly the declared primary-key fields.
  [[nodiscard]] static CanonicalBytes encodeKey(std::string_view schema,
                                                std::string_view table,
                                                const nlohmann::json &key);

  // A Debezium before/after image contains exactly the complete table row.
  [[nodiscard]] static CanonicalBytes
  encodeValue(std::string_view qualified_table, const nlohmann::json &row);

  [[nodiscard]] static const CanonicalRowCodec *
  tableCodec(std::string_view qualified_table) noexcept;
};

} // namespace blackbox
