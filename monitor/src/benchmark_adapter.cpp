#include "blackbox/benchmark_adapter.h"

#include "blackbox/canonical_ycsb_codec.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace blackbox {
namespace {

using Column = AdapterColumnContract;

struct ParsedSqlType {
  std::string_view base;
  std::optional<std::uint64_t> length;
  std::optional<std::uint64_t> scale;
  bool textual = false;
};

[[nodiscard]] std::uint64_t parseTypeNumber(const std::string_view value,
                                            const std::string_view sql_type) {
  std::uint64_t result = 0;
  const auto parsed =
      std::from_chars(value.data(), value.data() + value.size(), result);
  if (value.empty() || parsed.ec != std::errc{} ||
      parsed.ptr != value.data() + value.size()) {
    throw AdapterSchemaError("adapter SQL type is malformed: " +
                             std::string(sql_type));
  }
  return result;
}

[[nodiscard]] ParsedSqlType parseSqlType(const std::string_view sql_type) {
  const auto open = sql_type.find('(');
  if (open == std::string_view::npos) {
    return ParsedSqlType{.base = sql_type,
                         .length = std::nullopt,
                         .scale = std::nullopt,
                         .textual = false};
  }
  if (!sql_type.ends_with(')') || open == 0U) {
    throw AdapterSchemaError("adapter SQL type is malformed: " +
                             std::string(sql_type));
  }
  const auto base = sql_type.substr(0U, open);
  const auto parameters =
      sql_type.substr(open + 1U, sql_type.size() - open - 2U);
  if (base == "CHAR" || base == "VARCHAR") {
    return ParsedSqlType{.base = base,
                         .length = parseTypeNumber(parameters, sql_type),
                         .scale = std::nullopt,
                         .textual = true};
  }
  if (base == "DECIMAL") {
    const auto comma = parameters.find(',');
    if (comma == std::string_view::npos ||
        parameters.find(',', comma + 1U) != std::string_view::npos) {
      throw AdapterSchemaError("adapter SQL type is malformed: " +
                               std::string(sql_type));
    }
    return ParsedSqlType{
        .base = base,
        .length = parseTypeNumber(parameters.substr(0U, comma), sql_type),
        .scale = parseTypeNumber(parameters.substr(comma + 1U), sql_type),
        .textual = false};
  }
  throw AdapterSchemaError("adapter SQL type has unsupported parameters: " +
                           std::string(sql_type));
}

[[nodiscard]] std::string asciiLower(std::string value) {
  std::transform(
      value.begin(), value.end(), value.begin(), [](const char byte) {
        return byte >= 'A' && byte <= 'Z' ? static_cast<char>(byte - 'A' + 'a')
                                          : byte;
      });
  return value;
}

[[noreturn]] void schemaMismatch(const std::string_view table,
                                 const std::string_view reason) {
  throw AdapterSchemaError(
      "startup schema for " + std::string(table) +
      " disagrees with its adapter contract: " + std::string(reason));
}

[[nodiscard]] std::uint32_t readU32(const std::span<const std::byte> bytes,
                                    std::size_t &cursor,
                                    const std::string_view label) {
  if (cursor > bytes.size() || bytes.size() - cursor < 4U) {
    throw CanonicalEncodingError(std::string(label) +
                                 " has truncated U32 framing");
  }
  const auto byte = [&bytes](const std::size_t index) {
    return std::to_integer<std::uint32_t>(bytes[index]);
  };
  const auto result = (byte(cursor) << 24U) | (byte(cursor + 1U) << 16U) |
                      (byte(cursor + 2U) << 8U) | byte(cursor + 3U);
  cursor += 4U;
  return result;
}

[[nodiscard]] std::string readIdentifier(const std::span<const std::byte> bytes,
                                         std::size_t &cursor,
                                         const std::string_view label) {
  const auto size = static_cast<std::size_t>(readU32(bytes, cursor, label));
  if (cursor > bytes.size() || size > bytes.size() - cursor) {
    throw CanonicalEncodingError(std::string(label) +
                                 " has truncated LP framing");
  }
  std::string result;
  result.reserve(size);
  for (std::size_t index = 0; index < size; ++index) {
    result.push_back(static_cast<char>(
        std::to_integer<unsigned char>(bytes[cursor + index])));
  }
  cursor += size;
  if (result.empty() || result.size() > 64U || result.front() < 'a' ||
      result.front() > 'z' ||
      !std::all_of(result.begin() + 1, result.end(), [](const char character) {
        return (character >= 'a' && character <= 'z') ||
               (character >= '0' && character <= '9') || character == '_';
      })) {
    throw CanonicalEncodingError(std::string(label) +
                                 " is not a canonical identifier");
  }
  return result;
}

struct ParsedTablePrefix {
  std::string qualified_table;
  std::size_t suffix_offset = 0;
};

[[nodiscard]] ParsedTablePrefix
parseTablePrefix(const std::span<const std::byte> key) {
  std::size_t cursor = 0;
  auto schema = readIdentifier(key, cursor, "canonical key schema");
  auto table = readIdentifier(key, cursor, "canonical key table");
  return ParsedTablePrefix{.qualified_table = schema + "." + table,
                           .suffix_offset = cursor};
}

[[nodiscard]] bool validUtf8(const std::span<const std::byte> bytes) {
  const auto value = [&bytes](const std::size_t index) {
    return std::to_integer<unsigned int>(bytes[index]);
  };
  const auto continuation = [&value](const std::size_t index) {
    const auto current = value(index);
    return current >= 0x80U && current <= 0xbfU;
  };
  for (std::size_t index = 0; index < bytes.size();) {
    const auto first = value(index);
    if (first <= 0x7fU) {
      ++index;
      continue;
    }
    if (first >= 0xc2U && first <= 0xdfU) {
      if (index + 1U >= bytes.size() || !continuation(index + 1U)) {
        return false;
      }
      index += 2U;
      continue;
    }
    if (first >= 0xe0U && first <= 0xefU) {
      if (index + 2U >= bytes.size() || !continuation(index + 1U) ||
          !continuation(index + 2U)) {
        return false;
      }
      const auto second = value(index + 1U);
      if ((first == 0xe0U && second < 0xa0U) ||
          (first == 0xedU && second > 0x9fU)) {
        return false;
      }
      index += 3U;
      continue;
    }
    if (first >= 0xf0U && first <= 0xf4U) {
      if (index + 3U >= bytes.size() || !continuation(index + 1U) ||
          !continuation(index + 2U) || !continuation(index + 3U)) {
        return false;
      }
      const auto second = value(index + 1U);
      if ((first == 0xf0U && second < 0x90U) ||
          (first == 0xf4U && second > 0x8fU)) {
        return false;
      }
      index += 4U;
      continue;
    }
    return false;
  }
  return true;
}

class YcsbRowCodec final : public CanonicalRowCodec {
public:
  [[nodiscard]] std::string_view tableName() const noexcept override {
    return "benchbase.usertable";
  }

  void validateKey(const std::span<const std::byte> key) const override {
    const auto prefix = parseTablePrefix(key);
    if (prefix.qualified_table != tableName() ||
        prefix.suffix_offset > key.size() ||
        key.size() - prefix.suffix_offset != 4U) {
      throw CanonicalEncodingError(
          "YCSB canonical key does not end in exactly one I32");
    }
  }

  void
  validatePresentValue(const std::span<const std::byte> value) const override {
    std::size_t cursor = 0;
    for (std::size_t field = 0; field < CanonicalYcsbCodec::field_count;
         ++field) {
      if (cursor >= value.size()) {
        throw CanonicalEncodingError(
            "YCSB canonical value has fewer than ten fields");
      }
      const auto tag = std::to_integer<unsigned int>(value[cursor]);
      ++cursor;
      if (tag == 0U) {
        continue;
      }
      if (tag != 1U) {
        throw CanonicalEncodingError(
            "YCSB canonical value contains an invalid field tag");
      }
      const auto size = static_cast<std::size_t>(
          readU32(value, cursor, "YCSB canonical value field"));
      if (cursor > value.size() || size > value.size() - cursor) {
        throw CanonicalEncodingError(
            "YCSB canonical value contains a truncated field");
      }
      if (!validUtf8(value.subspan(cursor, size))) {
        throw CanonicalEncodingError(
            "YCSB canonical value contains malformed UTF-8");
      }
      cursor += size;
    }
    if (cursor != value.size()) {
      throw CanonicalEncodingError("YCSB canonical value has trailing bytes");
    }
  }
};

class StaticBenchmarkAdapter final : public BenchmarkAdapter {
public:
  constexpr StaticBenchmarkAdapter(
      const std::string_view adapter_name,
      const std::span<const std::string_view> transaction_types,
      const std::span<const std::string_view> accepted_isolations,
      const std::span<const AdapterTableContract> tables,
      const std::span<const AdapterResultSetContract> result_sets,
      const AdapterRuntimeCapabilities capabilities,
      const std::span<const CanonicalRowCodec *const> codecs)
      : name_(adapter_name), transaction_types_(transaction_types),
        accepted_isolations_(accepted_isolations), tables_(tables),
        result_sets_(result_sets), capabilities_(capabilities),
        codecs_(codecs) {}

  [[nodiscard]] std::string_view name() const noexcept override {
    return name_;
  }

  [[nodiscard]] std::span<const std::string_view>
  transactionTypes() const noexcept override {
    return transaction_types_;
  }

  [[nodiscard]] std::span<const std::string_view>
  acceptedIsolations() const noexcept override {
    return accepted_isolations_;
  }

  [[nodiscard]] std::span<const AdapterTableContract>
  tables() const noexcept override {
    return tables_;
  }

  [[nodiscard]] std::span<const AdapterResultSetContract>
  resultSets() const noexcept override {
    return result_sets_;
  }

  [[nodiscard]] AdapterRuntimeCapabilities
  runtimeCapabilities() const noexcept override {
    return capabilities_;
  }

  [[nodiscard]] const CanonicalRowCodec *
  rowCodec(const std::string_view qualified_table) const noexcept override {
    const auto found = std::find_if(
        codecs_.begin(), codecs_.end(),
        [qualified_table](const CanonicalRowCodec *codec) {
          return codec != nullptr && codec->tableName() == qualified_table;
        });
    return found == codecs_.end() ? nullptr : *found;
  }

private:
  std::string_view name_;
  std::span<const std::string_view> transaction_types_;
  std::span<const std::string_view> accepted_isolations_;
  std::span<const AdapterTableContract> tables_;
  std::span<const AdapterResultSetContract> result_sets_;
  AdapterRuntimeCapabilities capabilities_;
  std::span<const CanonicalRowCodec *const> codecs_;
};

constexpr std::array ycsb_transactions = {
    std::string_view{"ycsb_insert"},
    std::string_view{"ycsb_read"},
    std::string_view{"ycsb_update"},
    std::string_view{"ycsb_delete"},
    std::string_view{"si_fractured_read_probe"},
};
constexpr std::array ycsb_isolations = {
    std::string_view{"READ_COMMITTED"},
    std::string_view{"REPEATABLE_READ"},
};
constexpr std::array ycsb_primary_key = {std::string_view{"ycsb_key"}};
constexpr std::array ycsb_value_columns = {
    std::string_view{"field1"}, std::string_view{"field2"},
    std::string_view{"field3"}, std::string_view{"field4"},
    std::string_view{"field5"}, std::string_view{"field6"},
    std::string_view{"field7"}, std::string_view{"field8"},
    std::string_view{"field9"}, std::string_view{"field10"},
};
constexpr std::array ycsb_columns = {
    Column{"ycsb_key", "INT", false, true, false},
    Column{"field1", "VARCHAR(100)", true, false, false},
    Column{"field2", "VARCHAR(100)", true, false, false},
    Column{"field3", "VARCHAR(100)", true, false, false},
    Column{"field4", "VARCHAR(100)", true, false, false},
    Column{"field5", "VARCHAR(100)", true, false, false},
    Column{"field6", "VARCHAR(100)", true, false, false},
    Column{"field7", "VARCHAR(100)", true, false, false},
    Column{"field8", "VARCHAR(100)", true, false, false},
    Column{"field9", "VARCHAR(100)", true, false, false},
    Column{"field10", "VARCHAR(100)", true, false, false},
};
constexpr std::array ycsb_tables = {
    AdapterTableContract{"benchbase.usertable", ycsb_primary_key,
                         ycsb_value_columns, ycsb_columns},
};
constexpr std::array<AdapterResultSetContract, 0> ycsb_result_sets{};
const YcsbRowCodec ycsb_row_codec;
const std::array<const CanonicalRowCodec *, 1> ycsb_codecs = {&ycsb_row_codec};

constexpr std::array tpcc_transactions = {
    std::string_view{"NewOrder"},    std::string_view{"Payment"},
    std::string_view{"OrderStatus"}, std::string_view{"Delivery"},
    std::string_view{"StockLevel"},
};
constexpr std::array tpcc_isolations = {
    std::string_view{"REPEATABLE_READ"},
};

constexpr std::array warehouse_key = {std::string_view{"w_id"}};
constexpr std::array warehouse_values = {
    std::string_view{"w_ytd"},      std::string_view{"w_tax"},
    std::string_view{"w_name"},     std::string_view{"w_street_1"},
    std::string_view{"w_street_2"}, std::string_view{"w_city"},
    std::string_view{"w_state"},    std::string_view{"w_zip"},
};
constexpr std::array warehouse_columns = {
    Column{"w_id", "INT", false, true, false},
    Column{"w_ytd", "DECIMAL(12,2)", false, false, false},
    Column{"w_tax", "DECIMAL(4,4)", false, false, false},
    Column{"w_name", "VARCHAR(10)", false, false, false},
    Column{"w_street_1", "VARCHAR(20)", false, false, false},
    Column{"w_street_2", "VARCHAR(20)", false, false, false},
    Column{"w_city", "VARCHAR(20)", false, false, false},
    Column{"w_state", "CHAR(2)", false, false, false},
    Column{"w_zip", "CHAR(9)", false, false, false},
};

constexpr std::array item_key = {std::string_view{"i_id"}};
constexpr std::array item_values = {
    std::string_view{"i_name"},
    std::string_view{"i_price"},
    std::string_view{"i_data"},
    std::string_view{"i_im_id"},
};
constexpr std::array item_columns = {
    Column{"i_id", "INT", false, true, false},
    Column{"i_name", "VARCHAR(24)", false, false, false},
    Column{"i_price", "DECIMAL(5,2)", false, false, false},
    Column{"i_data", "VARCHAR(50)", false, false, false},
    Column{"i_im_id", "INT", false, false, false},
};

constexpr std::array stock_key = {std::string_view{"s_w_id"},
                                  std::string_view{"s_i_id"}};
constexpr std::array stock_values = {
    std::string_view{"s_quantity"},  std::string_view{"s_ytd"},
    std::string_view{"s_order_cnt"}, std::string_view{"s_remote_cnt"},
    std::string_view{"s_data"},      std::string_view{"s_dist_01"},
    std::string_view{"s_dist_02"},   std::string_view{"s_dist_03"},
    std::string_view{"s_dist_04"},   std::string_view{"s_dist_05"},
    std::string_view{"s_dist_06"},   std::string_view{"s_dist_07"},
    std::string_view{"s_dist_08"},   std::string_view{"s_dist_09"},
    std::string_view{"s_dist_10"},
};
constexpr std::array stock_columns = {
    Column{"s_w_id", "INT", false, true, false},
    Column{"s_i_id", "INT", false, true, false},
    Column{"s_quantity", "INT", false, false, false},
    Column{"s_ytd", "DECIMAL(8,2)", false, false, false},
    Column{"s_order_cnt", "INT", false, false, false},
    Column{"s_remote_cnt", "INT", false, false, false},
    Column{"s_data", "VARCHAR(50)", false, false, false},
    Column{"s_dist_01", "CHAR(24)", false, false, false},
    Column{"s_dist_02", "CHAR(24)", false, false, false},
    Column{"s_dist_03", "CHAR(24)", false, false, false},
    Column{"s_dist_04", "CHAR(24)", false, false, false},
    Column{"s_dist_05", "CHAR(24)", false, false, false},
    Column{"s_dist_06", "CHAR(24)", false, false, false},
    Column{"s_dist_07", "CHAR(24)", false, false, false},
    Column{"s_dist_08", "CHAR(24)", false, false, false},
    Column{"s_dist_09", "CHAR(24)", false, false, false},
    Column{"s_dist_10", "CHAR(24)", false, false, false},
};

constexpr std::array district_key = {std::string_view{"d_w_id"},
                                     std::string_view{"d_id"}};
constexpr std::array district_values = {
    std::string_view{"d_ytd"},       std::string_view{"d_tax"},
    std::string_view{"d_next_o_id"}, std::string_view{"d_name"},
    std::string_view{"d_street_1"},  std::string_view{"d_street_2"},
    std::string_view{"d_city"},      std::string_view{"d_state"},
    std::string_view{"d_zip"},
};
constexpr std::array district_columns = {
    Column{"d_w_id", "INT", false, true, false},
    Column{"d_id", "INT", false, true, false},
    Column{"d_ytd", "DECIMAL(12,2)", false, false, false},
    Column{"d_tax", "DECIMAL(4,4)", false, false, false},
    Column{"d_next_o_id", "INT", false, false, false},
    Column{"d_name", "VARCHAR(10)", false, false, false},
    Column{"d_street_1", "VARCHAR(20)", false, false, false},
    Column{"d_street_2", "VARCHAR(20)", false, false, false},
    Column{"d_city", "VARCHAR(20)", false, false, false},
    Column{"d_state", "CHAR(2)", false, false, false},
    Column{"d_zip", "CHAR(9)", false, false, false},
};

constexpr std::array customer_key = {std::string_view{"c_w_id"},
                                     std::string_view{"c_d_id"},
                                     std::string_view{"c_id"}};
constexpr std::array customer_values = {
    std::string_view{"c_discount"},     std::string_view{"c_credit"},
    std::string_view{"c_last"},         std::string_view{"c_first"},
    std::string_view{"c_credit_lim"},   std::string_view{"c_balance"},
    std::string_view{"c_ytd_payment"},  std::string_view{"c_payment_cnt"},
    std::string_view{"c_delivery_cnt"}, std::string_view{"c_street_1"},
    std::string_view{"c_street_2"},     std::string_view{"c_city"},
    std::string_view{"c_state"},        std::string_view{"c_zip"},
    std::string_view{"c_phone"},        std::string_view{"c_since"},
    std::string_view{"c_middle"},       std::string_view{"c_data"},
};
constexpr std::array customer_columns = {
    Column{"c_w_id", "INT", false, true, false},
    Column{"c_d_id", "INT", false, true, false},
    Column{"c_id", "INT", false, true, false},
    Column{"c_discount", "DECIMAL(4,4)", false, false, false},
    Column{"c_credit", "CHAR(2)", false, false, false},
    Column{"c_last", "VARCHAR(16)", false, false, false},
    Column{"c_first", "VARCHAR(16)", false, false, false},
    Column{"c_credit_lim", "DECIMAL(12,2)", false, false, false},
    Column{"c_balance", "DECIMAL(12,2)", false, false, false},
    Column{"c_ytd_payment", "FLOAT", false, false, false},
    Column{"c_payment_cnt", "INT", false, false, false},
    Column{"c_delivery_cnt", "INT", false, false, false},
    Column{"c_street_1", "VARCHAR(20)", false, false, false},
    Column{"c_street_2", "VARCHAR(20)", false, false, false},
    Column{"c_city", "VARCHAR(20)", false, false, false},
    Column{"c_state", "CHAR(2)", false, false, false},
    Column{"c_zip", "CHAR(9)", false, false, false},
    Column{"c_phone", "CHAR(16)", false, false, false},
    Column{"c_since", "TIMESTAMP", false, false, false},
    Column{"c_middle", "CHAR(2)", false, false, false},
    Column{"c_data", "VARCHAR(500)", false, false, false},
};

constexpr std::array history_key = {std::string_view{"h_id"}};
constexpr std::array history_values = {
    std::string_view{"h_c_id"},   std::string_view{"h_c_d_id"},
    std::string_view{"h_c_w_id"}, std::string_view{"h_d_id"},
    std::string_view{"h_w_id"},   std::string_view{"h_date"},
    std::string_view{"h_amount"}, std::string_view{"h_data"},
};
constexpr std::array history_columns = {
    Column{"h_id", "BIGINT", false, true, true},
    Column{"h_c_id", "INT", false, false, false},
    Column{"h_c_d_id", "INT", false, false, false},
    Column{"h_c_w_id", "INT", false, false, false},
    Column{"h_d_id", "INT", false, false, false},
    Column{"h_w_id", "INT", false, false, false},
    Column{"h_date", "TIMESTAMP", false, false, false},
    Column{"h_amount", "DECIMAL(6,2)", false, false, false},
    Column{"h_data", "VARCHAR(24)", false, false, false},
};

constexpr std::array oorder_key = {std::string_view{"o_w_id"},
                                   std::string_view{"o_d_id"},
                                   std::string_view{"o_id"}};
constexpr std::array oorder_values = {
    std::string_view{"o_c_id"},    std::string_view{"o_carrier_id"},
    std::string_view{"o_ol_cnt"},  std::string_view{"o_all_local"},
    std::string_view{"o_entry_d"},
};
constexpr std::array oorder_columns = {
    Column{"o_w_id", "INT", false, true, false},
    Column{"o_d_id", "INT", false, true, false},
    Column{"o_id", "INT", false, true, false},
    Column{"o_c_id", "INT", false, false, false},
    Column{"o_carrier_id", "INT", true, false, false},
    Column{"o_ol_cnt", "INT", false, false, false},
    Column{"o_all_local", "INT", false, false, false},
    Column{"o_entry_d", "TIMESTAMP", false, false, false},
};

constexpr std::array new_order_key = {std::string_view{"no_w_id"},
                                      std::string_view{"no_d_id"},
                                      std::string_view{"no_o_id"}};
constexpr std::array<std::string_view, 0> new_order_values{};
constexpr std::array new_order_columns = {
    Column{"no_w_id", "INT", false, true, false},
    Column{"no_d_id", "INT", false, true, false},
    Column{"no_o_id", "INT", false, true, false},
};

constexpr std::array order_line_key = {
    std::string_view{"ol_w_id"}, std::string_view{"ol_d_id"},
    std::string_view{"ol_o_id"}, std::string_view{"ol_number"}};
constexpr std::array order_line_values = {
    std::string_view{"ol_i_id"},     std::string_view{"ol_delivery_d"},
    std::string_view{"ol_amount"},   std::string_view{"ol_supply_w_id"},
    std::string_view{"ol_quantity"}, std::string_view{"ol_dist_info"},
};
constexpr std::array order_line_columns = {
    Column{"ol_w_id", "INT", false, true, false},
    Column{"ol_d_id", "INT", false, true, false},
    Column{"ol_o_id", "INT", false, true, false},
    Column{"ol_number", "INT", false, true, false},
    Column{"ol_i_id", "INT", false, false, false},
    Column{"ol_delivery_d", "TIMESTAMP", true, false, false},
    Column{"ol_amount", "DECIMAL(6,2)", false, false, false},
    Column{"ol_supply_w_id", "INT", false, false, false},
    Column{"ol_quantity", "INT", false, false, false},
    Column{"ol_dist_info", "CHAR(24)", false, false, false},
};

constexpr std::array tpcc_tables = {
    AdapterTableContract{"benchbase.warehouse", warehouse_key, warehouse_values,
                         warehouse_columns},
    AdapterTableContract{"benchbase.item", item_key, item_values, item_columns},
    AdapterTableContract{"benchbase.stock", stock_key, stock_values,
                         stock_columns},
    AdapterTableContract{"benchbase.district", district_key, district_values,
                         district_columns},
    AdapterTableContract{"benchbase.customer", customer_key, customer_values,
                         customer_columns},
    AdapterTableContract{"benchbase.history", history_key, history_values,
                         history_columns},
    AdapterTableContract{"benchbase.oorder", oorder_key, oorder_values,
                         oorder_columns},
    AdapterTableContract{"benchbase.new_order", new_order_key, new_order_values,
                         new_order_columns},
    AdapterTableContract{"benchbase.order_line", order_line_key,
                         order_line_values, order_line_columns},
};

constexpr std::array ignored_predicate = {std::string_view{"predicate"}};
constexpr std::array ignored_predicate_locking = {std::string_view{"predicate"},
                                                  std::string_view{"locking"}};
constexpr std::array ignored_predicate_ordering = {
    std::string_view{"predicate"}, std::string_view{"ordering"}};
constexpr std::array ignored_predicate_ordering_limit = {
    std::string_view{"predicate"}, std::string_view{"ordering"},
    std::string_view{"limit"}};
constexpr std::array ignored_predicate_aggregate = {
    std::string_view{"predicate"}, std::string_view{"aggregate"}};
constexpr std::array ignored_predicate_join_aggregate = {
    std::string_view{"predicate"}, std::string_view{"join"},
    std::string_view{"aggregate"}};

constexpr std::array tpcc_result_sets = {
    AdapterResultSetContract{
        "NewOrder", "NewOrder", "stmtGetCustSQL", "benchbase.customer",
        ResultSetClassification::Snapshot, ResultSetShape::ExactKeyRow,
        "absent", ignored_predicate, std::nullopt},
    AdapterResultSetContract{
        "NewOrder", "NewOrder", "stmtGetWhseSQL", "benchbase.warehouse",
        ResultSetClassification::Snapshot, ResultSetShape::ExactKeyRow,
        "absent", ignored_predicate, std::nullopt},
    AdapterResultSetContract{
        "NewOrder", "NewOrder", "stmtGetDistSQL", "benchbase.district",
        ResultSetClassification::Omit, ResultSetShape::ExactKeyRow, "no_event",
        ignored_predicate_locking, "locking_current_read"},
    AdapterResultSetContract{
        "NewOrder", "NewOrder", "stmtGetItemSQL", "benchbase.item",
        ResultSetClassification::Snapshot, ResultSetShape::ExactKeyRow,
        "absent", ignored_predicate, std::nullopt},
    AdapterResultSetContract{"NewOrder", "NewOrder", "stmtGetStockSQL",
                             "benchbase.stock", ResultSetClassification::Omit,
                             ResultSetShape::ExactKeyRow, "no_event",
                             ignored_predicate_locking, "locking_current_read"},
    AdapterResultSetContract{
        "Payment", "Payment", "payGetWhseSQL", "benchbase.warehouse",
        ResultSetClassification::Local, ResultSetShape::ExactKeyRow, "absent",
        ignored_predicate, std::nullopt},
    AdapterResultSetContract{
        "Payment", "Payment", "payGetDistSQL", "benchbase.district",
        ResultSetClassification::Local, ResultSetShape::ExactKeyRow, "absent",
        ignored_predicate, std::nullopt},
    AdapterResultSetContract{
        "Payment", "Payment", "payGetCustSQL", "benchbase.customer",
        ResultSetClassification::Snapshot, ResultSetShape::ExactKeyRow,
        "absent", ignored_predicate, std::nullopt},
    AdapterResultSetContract{
        "Payment", "Payment", "payGetCustCdataSQL", "benchbase.customer",
        ResultSetClassification::Snapshot, ResultSetShape::ExactKeyRow,
        "absent", ignored_predicate, std::nullopt},
    AdapterResultSetContract{
        "Payment", "Payment", "customerByNameSQL", "benchbase.customer",
        ResultSetClassification::Snapshot, ResultSetShape::ReturnedRows,
        "no_event", ignored_predicate_ordering, std::nullopt},
    AdapterResultSetContract{"OrderStatus", "OrderStatus",
                             "ordStatGetNewestOrdSQL", "benchbase.oorder",
                             ResultSetClassification::Snapshot,
                             ResultSetShape::ReturnedRows, "no_event",
                             ignored_predicate_ordering_limit, std::nullopt},
    AdapterResultSetContract{"OrderStatus", "OrderStatus",
                             "ordStatGetOrderLinesSQL", "benchbase.order_line",
                             ResultSetClassification::Snapshot,
                             ResultSetShape::ReturnedRows, "no_event",
                             ignored_predicate, std::nullopt},
    AdapterResultSetContract{
        "OrderStatus", "OrderStatus", "payGetCustSQL", "benchbase.customer",
        ResultSetClassification::Snapshot, ResultSetShape::ExactKeyRow,
        "absent", ignored_predicate, std::nullopt},
    AdapterResultSetContract{
        "OrderStatus", "OrderStatus", "customerByNameSQL", "benchbase.customer",
        ResultSetClassification::Snapshot, ResultSetShape::ReturnedRows,
        "no_event", ignored_predicate_ordering, std::nullopt},
    AdapterResultSetContract{
        "Delivery", "Delivery", "delivGetOrderIdSQL", "benchbase.new_order",
        ResultSetClassification::Snapshot, ResultSetShape::ReturnedRows,
        "no_event", ignored_predicate_ordering_limit, std::nullopt},
    AdapterResultSetContract{
        "Delivery", "Delivery", "delivGetCustIdSQL", "benchbase.oorder",
        ResultSetClassification::Snapshot, ResultSetShape::ExactKeyRow,
        "absent", ignored_predicate, std::nullopt},
    AdapterResultSetContract{"Delivery", "Delivery", "delivSumOrderAmountSQL",
                             "benchbase.order_line",
                             ResultSetClassification::Omit,
                             ResultSetShape::AggregateScalar, "not_applicable",
                             ignored_predicate_aggregate, "aggregate_scalar"},
    AdapterResultSetContract{
        "StockLevel", "StockLevel", "stockGetDistOrderIdSQL",
        "benchbase.district", ResultSetClassification::Snapshot,
        ResultSetShape::ExactKeyRow, "absent", ignored_predicate, std::nullopt},
    AdapterResultSetContract{
        "StockLevel", "StockLevel", "stockGetCountStockSQL", "benchbase.stock",
        ResultSetClassification::Omit, ResultSetShape::AggregateScalar,
        "not_applicable", ignored_predicate_join_aggregate, "aggregate_scalar"},
};
constexpr std::array<const CanonicalRowCodec *, 0> tpcc_codecs{};

const StaticBenchmarkAdapter
    ycsb_adapter("ycsb", ycsb_transactions, ycsb_isolations, ycsb_tables,
                 ycsb_result_sets,
                 AdapterRuntimeCapabilities{.client_trace = true,
                                            .cdc_normalization = true,
                                            .bootstrap_scan = true},
                 ycsb_codecs);

const StaticBenchmarkAdapter
    tpcc_adapter("tpcc", tpcc_transactions, tpcc_isolations, tpcc_tables,
                 tpcc_result_sets, AdapterRuntimeCapabilities{}, tpcc_codecs);

const std::array<const BenchmarkAdapter *, 2> registered_adapters = {
    &ycsb_adapter,
    &tpcc_adapter,
};

} // namespace

bool isProtocolName(const std::string_view value) noexcept {
  if (value.empty() || value.size() > 64U ||
      !((value.front() >= 'A' && value.front() <= 'Z') ||
        (value.front() >= 'a' && value.front() <= 'z'))) {
    return false;
  }
  return std::all_of(value.begin() + 1, value.end(), [](const char character) {
    return (character >= 'A' && character <= 'Z') ||
           (character >= 'a' && character <= 'z') ||
           (character >= '0' && character <= '9') || character == '_';
  });
}

bool BenchmarkAdapter::supportsTransaction(
    const std::string_view transaction) const {
  const auto transactions = transactionTypes();
  return std::find(transactions.begin(), transactions.end(), transaction) !=
         transactions.end();
}

bool BenchmarkAdapter::acceptsIsolation(
    const std::string_view isolation) const {
  const auto isolations = acceptedIsolations();
  return std::find(isolations.begin(), isolations.end(), isolation) !=
         isolations.end();
}

const AdapterTableContract *
BenchmarkAdapter::table(const std::string_view qualified_table) const noexcept {
  const auto contracts = tables();
  const auto found =
      std::find_if(contracts.begin(), contracts.end(),
                   [qualified_table](const AdapterTableContract &contract) {
                     return contract.name == qualified_table;
                   });
  return found == contracts.end() ? nullptr : &*found;
}

void BenchmarkAdapter::validateRunConfiguration(
    const std::string_view declared_isolation,
    const std::span<const std::string> application_table_allowlist,
    const bool require_runtime_capabilities) const {
  if (!acceptsIsolation(declared_isolation)) {
    throw MonitorError(
        MonitorErrorCode::IsolationMismatch,
        "declared isolation is not accepted by benchmark adapter " +
            std::string(name()));
  }

  validateApplicationTableRoster(application_table_allowlist);

  if (require_runtime_capabilities && !runtimeCapabilities().complete()) {
    throw MonitorError(
        MonitorErrorCode::BranchContractMismatch,
        "benchmark adapter lacks complete monitor runtime capabilities: " +
            std::string(name()));
  }
}

void BenchmarkAdapter::validateApplicationTableRoster(
    const std::span<const std::string> application_table_allowlist) const {
  std::vector<std::string_view> expected;
  expected.reserve(tables().size());
  for (const auto &contract : tables()) {
    expected.push_back(contract.name);
  }
  std::vector<std::string_view> actual;
  actual.reserve(application_table_allowlist.size());
  for (const auto &table_name : application_table_allowlist) {
    actual.push_back(table_name);
  }
  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());
  if (expected != actual ||
      std::adjacent_find(actual.begin(), actual.end()) != actual.end()) {
    throw MonitorError(
        MonitorErrorCode::UnexpectedTable,
        "application table roster differs from benchmark adapter " +
            std::string(name()));
  }
}

void BenchmarkAdapter::validateStartupTableSchema(
    const StartupTableSchema &schema,
    const ApplicationTableRequirements &backend_requirements) const {
  const auto *expected_table = table(schema.name);
  if (expected_table == nullptr) {
    schemaMismatch(schema.name, "table is outside the adapter roster");
  }

  if (!schema.default_charset_name.has_value() ||
      *schema.default_charset_name != backend_requirements.character_set) {
    schemaMismatch(schema.name, "default character set differs");
  }

  const auto ddl = asciiLower(schema.ddl);
  const auto final_parenthesis = ddl.rfind(')');
  if (final_parenthesis == std::string::npos) {
    schemaMismatch(schema.name, "DDL lacks a table-options suffix");
  }
  const auto options = std::string_view(ddl).substr(final_parenthesis + 1U);
  const auto required_engine =
      "engine=" + asciiLower(std::string(backend_requirements.engine));
  const auto required_charset =
      "default charset=" +
      asciiLower(std::string(backend_requirements.character_set));
  const auto required_collation =
      "collate=" + asciiLower(std::string(backend_requirements.collation));
  if (options.find(required_engine) == std::string_view::npos ||
      options.find(required_charset) == std::string_view::npos ||
      options.find(required_collation) == std::string_view::npos) {
    schemaMismatch(schema.name,
                   "DDL table engine/character set/collation differs");
  }
  if (backend_requirements.char_read_semantics != "pad_to_declared_length") {
    throw AdapterSchemaError(
        "backend CHAR read semantics are unsupported by the adapter boundary");
  }

  if (schema.primary_key_columns.size() != expected_table->primary_key.size() ||
      !std::equal(schema.primary_key_columns.begin(),
                  schema.primary_key_columns.end(),
                  expected_table->primary_key.begin())) {
    schemaMismatch(schema.name, "primary-key columns or order differ");
  }
  if (schema.columns.size() != expected_table->columns.size()) {
    schemaMismatch(schema.name, "column count differs");
  }

  for (std::size_t index = 0; index < expected_table->columns.size(); ++index) {
    const auto &expected = expected_table->columns[index];
    const auto &actual = schema.columns[index];
    const auto expected_type = parseSqlType(expected.sql_type);
    if (actual.name != expected.name || actual.position != index + 1U) {
      schemaMismatch(schema.name, "column name or ordinal differs");
    }
    if (actual.type_name != expected_type.base ||
        actual.type_expression != expected_type.base) {
      schemaMismatch(schema.name, "column SQL type differs");
    }
    if (expected_type.length.has_value() &&
        actual.length != expected_type.length) {
      schemaMismatch(schema.name, "column length/precision differs");
    }
    if (actual.scale != expected_type.scale) {
      schemaMismatch(schema.name, "column scale differs");
    }
    if (actual.nullable != expected.nullable) {
      schemaMismatch(schema.name, "column nullability differs");
    }
    const bool expected_auto_increment =
        expected.generated == std::optional<std::string_view>{"auto_increment"};
    if (actual.auto_incremented != expected_auto_increment ||
        actual.generated != expected_auto_increment) {
      schemaMismatch(schema.name, "column generated metadata differs");
    }
    if (expected_type.textual) {
      if (!actual.charset_name.has_value() ||
          *actual.charset_name != backend_requirements.character_set) {
        schemaMismatch(schema.name, "text column character set differs");
      }
    } else if (actual.charset_name.has_value()) {
      schemaMismatch(schema.name,
                     "non-text column unexpectedly has a character set");
    }
  }
}

void BenchmarkAdapter::validateClientRead(
    const std::span<const std::byte> key,
    const std::optional<std::span<const std::byte>> value) const {
  try {
    const auto prefix = parseTablePrefix(key);
    if (table(prefix.qualified_table) == nullptr) {
      throw CanonicalEncodingError(
          "canonical client key names a table outside the adapter roster");
    }
    const auto *codec = rowCodec(prefix.qualified_table);
    if (codec == nullptr || !runtimeCapabilities().client_trace) {
      throw MonitorError(
          MonitorErrorCode::BranchContractMismatch,
          "benchmark table lacks a client canonical-row codec: " +
              prefix.qualified_table);
    }
    codec->validateKey(key);
    if (value.has_value()) {
      codec->validatePresentValue(*value);
    }
  } catch (const MonitorError &) {
    throw;
  } catch (const CanonicalEncodingError &error) {
    throw MonitorError(MonitorErrorCode::CanonicalEncodingFailed,
                       std::string("invalid adapter-owned client bytes: ") +
                           error.what());
  }
}

std::span<const BenchmarkAdapter *const>
BenchmarkAdapterRegistry::adapters() noexcept {
  return registered_adapters;
}

const BenchmarkAdapter *
BenchmarkAdapterRegistry::find(const std::string_view benchmark) noexcept {
  const auto found =
      std::find_if(registered_adapters.begin(), registered_adapters.end(),
                   [benchmark](const BenchmarkAdapter *adapter) {
                     return adapter != nullptr && adapter->name() == benchmark;
                   });
  return found == registered_adapters.end() ? nullptr : *found;
}

const BenchmarkAdapter &
BenchmarkAdapterRegistry::require(const std::string_view benchmark) {
  const auto *adapter = find(benchmark);
  if (adapter == nullptr) {
    throw MonitorError(MonitorErrorCode::ConfigInvalid,
                       "RunManifest names an unregistered benchmark adapter");
  }
  return *adapter;
}

std::string_view
toString(const ResultSetClassification classification) noexcept {
  switch (classification) {
  case ResultSetClassification::Snapshot:
    return "snapshot";
  case ResultSetClassification::Local:
    return "local";
  case ResultSetClassification::Omit:
    return "omit";
  }
  std::abort();
}

std::string_view toString(const ResultSetShape shape) noexcept {
  switch (shape) {
  case ResultSetShape::ExactKeyRow:
    return "exact_key_row";
  case ResultSetShape::ReturnedRows:
    return "returned_rows";
  case ResultSetShape::AggregateScalar:
    return "aggregate_scalar";
  }
  std::abort();
}

} // namespace blackbox
