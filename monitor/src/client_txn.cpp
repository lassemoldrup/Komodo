#include "blackbox/client_txn.h"

#include <openssl/evp.h>
#include <simdjson.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>

namespace blackbox {
namespace {

constexpr std::uint64_t max_json_integer = 9007199254740991ULL;
constexpr std::size_t max_client_json_depth = 8U;
constexpr std::array<char, 26> read_digest_domain = {
    'd', 'b', '-', 'm', 'o', 'n', 'i', 't', 'o', 'r', 'i', 'n', 'g',
    '/', 'r', 'e', 'a', 'd', '-', 'd', 'i', 'g', 'e', 's', 't', '\0'};

[[noreturn]] void malformed(const std::string &message) {
  throw MonitorError(MonitorErrorCode::ClientRecordMalformed, message);
}

[[nodiscard]] std::string
stringValue(simdjson::ondemand::value value, const std::string_view label) {
  try {
    return std::string(std::string_view(value.get_string()));
  } catch (const simdjson::simdjson_error &) {
    malformed(std::string(label) + " is not a string");
  }
}

[[nodiscard]] std::uint64_t
uintValue(simdjson::ondemand::value value, const std::string_view label,
          const std::uint64_t maximum, const bool positive) {
  auto token = value.raw_json_token();
  while (!token.empty() &&
         (token.front() == ' ' || token.front() == '\t' ||
          token.front() == '\n' || token.front() == '\r')) {
    token.remove_prefix(1U);
  }
  while (!token.empty() &&
         (token.back() == ' ' || token.back() == '\t' ||
          token.back() == '\n' || token.back() == '\r')) {
    token.remove_suffix(1U);
  }
  double numeric = 0.0;
  const auto parsed = std::from_chars(token.data(), token.data() + token.size(),
                                      numeric, std::chars_format::general);
  if (parsed.ec != std::errc{} || parsed.ptr != token.data() + token.size() ||
      !std::isfinite(numeric) || numeric < 0.0 ||
      std::trunc(numeric) != numeric ||
      numeric > static_cast<double>(maximum)) {
    malformed(std::string(label) + " is not a supported integer");
  }
  const auto result = static_cast<std::uint64_t>(numeric);
  if ((positive && result == 0U) || result > maximum) {
    malformed(std::string(label) + " is outside its supported range");
  }
  return result;
}

void markField(std::uint32_t &seen, const std::uint32_t bit,
               const std::string_view key, const std::string_view label) {
  if ((seen & bit) != 0U) {
    malformed(std::string(label) + " contains duplicated field " +
              std::string(key));
  }
  seen |= bit;
}

void requireFields(const std::uint32_t seen, const std::uint32_t expected,
                   const std::string_view label) {
  if (seen != expected) {
    malformed(std::string(label) + " is missing a required field");
  }
}

[[nodiscard]] bool isLowerUuid(const std::string_view value) {
  if (value.size() != 36U) {
    return false;
  }
  for (std::size_t index = 0; index < value.size(); ++index) {
    if (index == 8U || index == 13U || index == 18U || index == 23U) {
      if (value[index] != '-') {
        return false;
      }
      continue;
    }
    if (!(value[index] >= '0' && value[index] <= '9') &&
        !(value[index] >= 'a' && value[index] <= 'f')) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] bool isSessionId(const std::string_view value) {
  if (value.empty() || value.size() > 256U) {
    return false;
  }
  const auto legal = [](const char character) {
    return (character >= 'A' && character <= 'Z') ||
           (character >= 'a' && character <= 'z') ||
           (character >= '0' && character <= '9') || character == '.' ||
           character == '_' || character == ':' || character == '/' ||
           character == '-';
  };
  const auto first = value.front();
  const bool legal_first = (first >= 'A' && first <= 'Z') ||
                           (first >= 'a' && first <= 'z') ||
                           (first >= '0' && first <= '9');
  return legal_first && std::all_of(value.begin(), value.end(), legal);
}

// JSON Schema treats 1.0 and 1e0 as integers, but a binary64 DOM alone cannot
// distinguish them from a fractional token rounded to an integral double.
// Check every numeric lexeme before DOM conversion; ClientTxn has no
// non-integer numeric fields.
void validateIntegralNumberLexemes(const std::string_view input) {
  std::size_t depth = 0U;
  for (std::size_t cursor = 0; cursor < input.size();) {
    if (input[cursor] == '"') {
      ++cursor;
      while (cursor < input.size()) {
        if (input[cursor] == '\\') {
          cursor += std::min<std::size_t>(2U, input.size() - cursor);
        } else if (input[cursor] == '"') {
          ++cursor;
          break;
        } else {
          ++cursor;
        }
      }
      continue;
    }
    if (input[cursor] == '{' || input[cursor] == '[') {
      ++depth;
      if (depth > max_client_json_depth) {
        throw MonitorError(MonitorErrorCode::ResourceExhausted,
                           "ClientTxn JSON exceeds its depth limit");
      }
      ++cursor;
      continue;
    }
    if (input[cursor] == '}' || input[cursor] == ']') {
      if (depth != 0U) {
        --depth;
      }
      ++cursor;
      continue;
    }
    if (input[cursor] != '-' &&
        !(input[cursor] >= '0' && input[cursor] <= '9')) {
      ++cursor;
      continue;
    }

    std::size_t number_cursor = cursor;
    if (input[number_cursor] == '-') {
      ++number_cursor;
    }
    const auto integer_begin = number_cursor;
    while (number_cursor < input.size() && input[number_cursor] >= '0' &&
           input[number_cursor] <= '9') {
      ++number_cursor;
    }
    const auto integer_end = number_cursor;
    if (integer_begin == integer_end) {
      malformed("ClientTxn JSON contains an invalid number");
    }

    std::size_t fraction_begin = number_cursor;
    std::size_t fraction_end = number_cursor;
    if (number_cursor < input.size() && input[number_cursor] == '.') {
      ++number_cursor;
      fraction_begin = number_cursor;
      while (number_cursor < input.size() && input[number_cursor] >= '0' &&
             input[number_cursor] <= '9') {
        ++number_cursor;
      }
      fraction_end = number_cursor;
      if (fraction_begin == fraction_end) {
        malformed("ClientTxn JSON contains an invalid fraction");
      }
    }

    bool exponent_negative = false;
    std::size_t exponent = 0;
    if (number_cursor < input.size() &&
        (input[number_cursor] == 'e' || input[number_cursor] == 'E')) {
      ++number_cursor;
      if (number_cursor < input.size() &&
          (input[number_cursor] == '+' || input[number_cursor] == '-')) {
        exponent_negative = input[number_cursor] == '-';
        ++number_cursor;
      }
      const auto exponent_begin = number_cursor;
      const auto exponent_cap = input.size() + 1U;
      while (number_cursor < input.size() && input[number_cursor] >= '0' &&
             input[number_cursor] <= '9') {
        const auto digit = static_cast<std::size_t>(input[number_cursor] - '0');
        if (exponent < exponent_cap) {
          exponent = exponent > (exponent_cap - digit) / 10U
                         ? exponent_cap
                         : std::min(exponent * 10U + digit, exponent_cap);
        }
        ++number_cursor;
      }
      if (exponent_begin == number_cursor) {
        malformed("ClientTxn JSON contains an invalid exponent");
      }
    }

    const auto all_zero = [input](const std::size_t begin,
                                  const std::size_t end) {
      return std::all_of(input.begin() + static_cast<std::ptrdiff_t>(begin),
                         input.begin() + static_cast<std::ptrdiff_t>(end),
                         [](const char character) { return character == '0'; });
    };
    if (!all_zero(integer_begin, integer_end) ||
        !all_zero(fraction_begin, fraction_end)) {
      const auto fraction_digits = fraction_end - fraction_begin;
      std::size_t decimal_scale = 0;
      if (exponent_negative) {
        decimal_scale =
            exponent > input.size() - std::min(fraction_digits, input.size())
                ? input.size() + 1U
                : fraction_digits + exponent;
      } else if (exponent < fraction_digits) {
        decimal_scale = fraction_digits - exponent;
      }

      if (decimal_scale != 0U) {
        std::size_t trailing_zeroes = 0;
        for (auto index = fraction_end;
             index > fraction_begin && input[index - 1U] == '0'; --index) {
          ++trailing_zeroes;
        }
        if (trailing_zeroes == fraction_digits) {
          for (auto index = integer_end;
               index > integer_begin && input[index - 1U] == '0'; --index) {
            ++trailing_zeroes;
          }
        }
        if (trailing_zeroes < decimal_scale) {
          malformed("ClientTxn JSON contains a fractional counter");
        }
      }
    }
    cursor = number_cursor;
  }
}

[[nodiscard]] int base64Value(const char character) noexcept {
  if (character >= 'A' && character <= 'Z') {
    return character - 'A';
  }
  if (character >= 'a' && character <= 'z') {
    return character - 'a' + 26;
  }
  if (character >= '0' && character <= '9') {
    return character - '0' + 52;
  }
  if (character == '+') {
    return 62;
  }
  if (character == '/') {
    return 63;
  }
  return -1;
}

[[nodiscard]] CanonicalBytes decodeBase64(const std::string_view encoded,
                                          const std::string_view label,
                                          const std::size_t maximum) {
  if (encoded.size() % 4U != 0U) {
    malformed(std::string(label) + " is not padded RFC 4648 Base64");
  }
  const std::size_t padding =
      encoded.empty()
          ? 0U
          : (encoded.back() == '='
                 ? (encoded.size() >= 2U && encoded[encoded.size() - 2U] == '='
                        ? 2U
                        : 1U)
                 : 0U);
  const std::size_t decoded_size = encoded.size() / 4U * 3U - padding;
  if (decoded_size > maximum) {
    throw MonitorError(MonitorErrorCode::ResourceExhausted,
                       std::string(label) + " exceeds its decoded byte limit");
  }

  CanonicalBytes decoded;
  decoded.reserve(decoded_size);
  for (std::size_t index = 0; index < encoded.size(); index += 4U) {
    const bool last = index + 4U == encoded.size();
    const int first = base64Value(encoded[index]);
    const int second = base64Value(encoded[index + 1U]);
    if (first < 0 || second < 0) {
      malformed(std::string(label) + " contains an invalid Base64 character");
    }
    const char third_character = encoded[index + 2U];
    const char fourth_character = encoded[index + 3U];
    const int third = base64Value(third_character);
    const int fourth = base64Value(fourth_character);
    if (third_character == '=') {
      if (!last || fourth_character != '=' || (second & 0x0f) != 0) {
        malformed(std::string(label) + " has non-canonical Base64 padding");
      }
      decoded.push_back(static_cast<std::byte>((first << 2) | (second >> 4)));
      continue;
    }
    if (third < 0) {
      malformed(std::string(label) + " contains an invalid Base64 character");
    }
    decoded.push_back(static_cast<std::byte>((first << 2) | (second >> 4)));
    decoded.push_back(
        static_cast<std::byte>(((second & 0x0f) << 4) | (third >> 2)));
    if (fourth_character == '=') {
      if (!last || (third & 0x03) != 0) {
        malformed(std::string(label) + " has non-canonical Base64 padding");
      }
      continue;
    }
    if (fourth < 0) {
      malformed(std::string(label) + " contains an invalid Base64 character");
    }
    decoded.push_back(static_cast<std::byte>(((third & 0x03) << 6) | fourth));
  }
  if (decoded.size() != decoded_size) {
    malformed(std::string(label) + " has an invalid Base64 length");
  }
  return decoded;
}

class DigestContext final {
public:
  DigestContext()
      : context_(EVP_MD_CTX_new(),
                 [](EVP_MD_CTX *context) { EVP_MD_CTX_free(context); }) {
    if (!context_) {
      throw MonitorError(MonitorErrorCode::ResourceExhausted,
                         "could not allocate a SHA-256 read digest context");
    }
    if (EVP_DigestInit_ex(context_.get(), EVP_sha256(), nullptr) != 1) {
      throw MonitorError(MonitorErrorCode::InternalError,
                         "could not initialize SHA-256 read digest");
    }
  }

  void update(const void *data, const std::size_t size) {
    if (size != 0U && EVP_DigestUpdate(context_.get(), data, size) != 1) {
      throw MonitorError(MonitorErrorCode::InternalError,
                         "could not update SHA-256 read digest");
    }
  }

  void update(const std::span<const std::byte> bytes) {
    update(bytes.data(), bytes.size());
  }

  [[nodiscard]] std::string finish() {
    std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
    unsigned int size = 0;
    if (EVP_DigestFinal_ex(context_.get(), digest.data(), &size) != 1 ||
        size != 32U) {
      throw MonitorError(MonitorErrorCode::InternalError,
                         "could not finish SHA-256 read digest");
    }
    constexpr std::array<char, 16> hex = {'0', '1', '2', '3', '4', '5',
                                          '6', '7', '8', '9', 'a', 'b',
                                          'c', 'd', 'e', 'f'};
    std::string result;
    result.resize(size * 2U);
    for (std::size_t index = 0; index < size; ++index) {
      result[index * 2U] = hex[digest[index] >> 4U];
      result[index * 2U + 1U] = hex[digest[index] & 0x0fU];
    }
    return result;
  }

private:
  std::unique_ptr<EVP_MD_CTX, void (*)(EVP_MD_CTX *)> context_;
};

void updateU32(DigestContext &digest, const std::uint32_t value) {
  const std::array<unsigned char, 4> encoded = {
      static_cast<unsigned char>((value >> 24U) & 0xffU),
      static_cast<unsigned char>((value >> 16U) & 0xffU),
      static_cast<unsigned char>((value >> 8U) & 0xffU),
      static_cast<unsigned char>(value & 0xffU),
  };
  digest.update(encoded.data(), encoded.size());
}

void updateLengthPrefixed(DigestContext &digest,
                          const std::span<const std::byte> bytes) {
  if (bytes.size() > std::numeric_limits<std::uint32_t>::max()) {
    throw MonitorError(MonitorErrorCode::ResourceExhausted,
                       "read digest LP payload exceeds uint32");
  }
  updateU32(digest, static_cast<std::uint32_t>(bytes.size()));
  digest.update(bytes);
}

[[nodiscard]] bool isSha256(const std::string_view value) {
  return value.size() == 64U &&
         std::all_of(value.begin(), value.end(), [](const char character) {
           return (character >= '0' && character <= '9') ||
                  (character >= 'a' && character <= 'f');
         });
}

} // namespace

std::string computeClientReadDigest(const std::span<const ClientRead> reads) {
  DigestContext digest;
  digest.update(read_digest_domain.data(), read_digest_domain.size());
  if (reads.size() > std::numeric_limits<std::uint32_t>::max()) {
    throw MonitorError(MonitorErrorCode::ResourceExhausted,
                       "read count exceeds digest framing range");
  }
  updateU32(digest, static_cast<std::uint32_t>(reads.size()));
  for (std::size_t index = 0; index < reads.size(); ++index) {
    const auto &read = reads[index];
    if (read.event_seq != index || read.key.empty()) {
      throw MonitorError(
          MonitorErrorCode::ClientRecordMalformed,
          "read digest input lacks dense sequence or a nonempty key");
    }
    updateU32(digest, read.event_seq);
    updateLengthPrefixed(digest, read.key);
    unsigned char state = 0;
    switch (read.state) {
    case ClientValueState::Present:
      state = 0x01U;
      if (!read.value.has_value()) {
        throw MonitorError(MonitorErrorCode::ClientRecordMalformed,
                           "present read digest input lacks a value");
      }
      break;
    case ClientValueState::Absent:
      state = 0x02U;
      if (read.value.has_value()) {
        throw MonitorError(MonitorErrorCode::ClientRecordMalformed,
                           "absent read digest input carries a value");
      }
      break;
    default:
      throw MonitorError(MonitorErrorCode::ClientRecordMalformed,
                         "read digest input has an invalid value state");
    }
    digest.update(&state, 1U);
    if (read.state == ClientValueState::Present) {
      updateLengthPrefixed(digest, *read.value);
    }
    unsigned char source = 0;
    switch (read.source) {
    case ClientReadSource::Snapshot:
      source = 0x01U;
      break;
    case ClientReadSource::Local:
      source = 0x02U;
      break;
    default:
      throw MonitorError(MonitorErrorCode::UnsupportedReadSource,
                         "read digest input has an invalid source");
    }
    digest.update(&source, 1U);
  }
  return digest.finish();
}

ClientTxnParser::ClientTxnParser(std::string run_id,
                                 std::vector<std::string> expected_session_ids,
                                 const BenchmarkAdapter &benchmark_adapter,
                                 const ClientTxnParserLimits limits)
    : run_id_(std::move(run_id)), benchmark_adapter_(&benchmark_adapter),
      limits_(limits) {
  if (!isLowerUuid(run_id_)) {
    throw std::invalid_argument("client parser run_id is not a lowercase UUID");
  }
  if (expected_session_ids.empty()) {
    throw std::invalid_argument(
        "client parser expected-session roster is empty");
  }
  for (auto &session_id : expected_session_ids) {
    if (!isSessionId(session_id) ||
        !expected_session_ids_.insert(std::move(session_id)).second) {
      throw std::invalid_argument(
          "client parser expected-session roster is invalid or duplicated");
    }
  }
  if (limits_.max_payload_bytes == 0U || limits_.max_reads == 0U ||
      limits_.max_reads > 1000U || limits_.max_key_bytes == 0U ||
      limits_.max_value_bytes == 0U || limits_.max_decoded_bytes == 0U) {
    throw std::invalid_argument("client parser limits are invalid");
  }
}

ClientTxn ClientTxnParser::parse(const std::string_view payload) const {
  if (payload.size() > limits_.max_payload_bytes) {
    throw MonitorError(MonitorErrorCode::ResourceExhausted,
                       "client transaction payload exceeds its byte limit");
  }
  try {
    validateIntegralNumberLexemes(payload);
    thread_local simdjson::ondemand::parser parser;
    const simdjson::padded_string padded(payload);
    simdjson::ondemand::document document = parser.iterate(padded);
    simdjson::ondemand::object object = document.get_object();

    constexpr std::uint32_t kind_bit = 1U << 0U;
    constexpr std::uint32_t run_id_bit = 1U << 1U;
    constexpr std::uint32_t session_id_bit = 1U << 2U;
    constexpr std::uint32_t session_seq_bit = 1U << 3U;
    constexpr std::uint32_t logical_txn_id_bit = 1U << 4U;
    constexpr std::uint32_t attempt_id_bit = 1U << 5U;
    constexpr std::uint32_t txn_type_bit = 1U << 6U;
    constexpr std::uint32_t correlation_kind_bit = 1U << 7U;
    constexpr std::uint32_t correlation_id_bit = 1U << 8U;
    constexpr std::uint32_t read_count_bit = 1U << 9U;
    constexpr std::uint32_t read_digest_bit = 1U << 10U;
    constexpr std::uint32_t reads_bit = 1U << 11U;
    constexpr std::uint32_t all_fields = (1U << 12U) - 1U;

    std::uint32_t seen = 0U;
    std::string kind;
    std::string run_id;
    std::string session_id;
    std::uint64_t session_seq = 0U;
    std::string logical_txn_id;
    std::string attempt_id;
    std::string txn_type;
    std::string correlation_kind;
    std::string correlation_run;
    std::string correlation_session;
    std::uint64_t txn_id = 0U;
    std::uint64_t read_count = 0U;
    std::string read_digest;
    std::vector<ClientRead> reads;
    std::size_t decoded_bytes = 0U;

    for (auto field : object) {
      const std::string_view key = field.unescaped_key();
      simdjson::ondemand::value value = field.value();
      if (key == "kind") {
        markField(seen, kind_bit, key, "ClientTxn");
        kind = stringValue(value, "ClientTxn.kind");
      } else if (key == "run_id") {
        markField(seen, run_id_bit, key, "ClientTxn");
        run_id = stringValue(value, "ClientTxn.run_id");
      } else if (key == "session_id") {
        markField(seen, session_id_bit, key, "ClientTxn");
        session_id = stringValue(value, "ClientTxn.session_id");
      } else if (key == "session_seq") {
        markField(seen, session_seq_bit, key, "ClientTxn");
        session_seq =
            uintValue(value, "ClientTxn.session_seq", max_json_integer, true);
      } else if (key == "logical_txn_id") {
        markField(seen, logical_txn_id_bit, key, "ClientTxn");
        logical_txn_id = stringValue(value, "ClientTxn.logical_txn_id");
      } else if (key == "attempt_id") {
        markField(seen, attempt_id_bit, key, "ClientTxn");
        attempt_id = stringValue(value, "ClientTxn.attempt_id");
      } else if (key == "txn_type") {
        markField(seen, txn_type_bit, key, "ClientTxn");
        txn_type = stringValue(value, "ClientTxn.txn_type");
      } else if (key == "correlation_kind") {
        markField(seen, correlation_kind_bit, key, "ClientTxn");
        correlation_kind =
            stringValue(value, "ClientTxn.correlation_kind");
      } else if (key == "correlation_id") {
        markField(seen, correlation_id_bit, key, "ClientTxn");
        simdjson::ondemand::object correlation = value.get_object();
        constexpr std::uint32_t correlation_run_bit = 1U << 0U;
        constexpr std::uint32_t correlation_session_bit = 1U << 1U;
        constexpr std::uint32_t correlation_txn_bit = 1U << 2U;
        constexpr std::uint32_t correlation_fields = (1U << 3U) - 1U;
        std::uint32_t correlation_seen = 0U;
        for (auto correlation_field : correlation) {
          const std::string_view correlation_key =
              correlation_field.unescaped_key();
          simdjson::ondemand::value correlation_value =
              correlation_field.value();
          if (correlation_key == "run_id") {
            markField(correlation_seen, correlation_run_bit, correlation_key,
                      "ClientTxn.correlation_id");
            correlation_run = stringValue(
                correlation_value, "ClientTxn.correlation_id.run_id");
          } else if (correlation_key == "session_id") {
            markField(correlation_seen, correlation_session_bit,
                      correlation_key, "ClientTxn.correlation_id");
            correlation_session = stringValue(
                correlation_value, "ClientTxn.correlation_id.session_id");
          } else if (correlation_key == "txn_id") {
            markField(correlation_seen, correlation_txn_bit, correlation_key,
                      "ClientTxn.correlation_id");
            txn_id = uintValue(correlation_value,
                               "ClientTxn.correlation_id.txn_id",
                               max_json_integer, true);
          } else {
            malformed("ClientTxn.correlation_id contains unexpected field " +
                      std::string(correlation_key));
          }
        }
        requireFields(correlation_seen, correlation_fields,
                      "ClientTxn.correlation_id");
      } else if (key == "read_count") {
        markField(seen, read_count_bit, key, "ClientTxn");
        read_count = uintValue(value, "ClientTxn.read_count",
                               std::numeric_limits<std::uint32_t>::max(),
                               false);
      } else if (key == "read_digest") {
        markField(seen, read_digest_bit, key, "ClientTxn");
        read_digest = stringValue(value, "ClientTxn.read_digest");
      } else if (key == "reads") {
        markField(seen, reads_bit, key, "ClientTxn");
        simdjson::ondemand::array read_array = value.get_array();
        for (auto read_value : read_array) {
          if (reads.size() >= limits_.max_reads) {
            throw MonitorError(
                MonitorErrorCode::ResourceExhausted,
                "ClientTxn read count exceeds its configured limit");
          }
          simdjson::ondemand::object read_object = read_value.get_object();
          constexpr std::uint32_t event_seq_bit = 1U << 0U;
          constexpr std::uint32_t key_bytes_bit = 1U << 1U;
          constexpr std::uint32_t value_state_bit = 1U << 2U;
          constexpr std::uint32_t value_bytes_bit = 1U << 3U;
          constexpr std::uint32_t source_bit = 1U << 4U;
          constexpr std::uint32_t absent_fields =
              event_seq_bit | key_bytes_bit | value_state_bit | source_bit;
          constexpr std::uint32_t present_fields =
              absent_fields | value_bytes_bit;
          std::uint32_t read_seen = 0U;
          std::uint64_t event_seq = 0U;
          std::optional<CanonicalBytes> decoded_key;
          std::optional<CanonicalBytes> decoded_value;
          std::string state_text;
          std::string source_text;

          for (auto read_field : read_object) {
            const std::string_view read_key = read_field.unescaped_key();
            simdjson::ondemand::value field_value = read_field.value();
            if (read_key == "event_seq") {
              markField(read_seen, event_seq_bit, read_key, "ClientTxn read");
              event_seq = uintValue(
                  field_value, "ClientTxn read.event_seq",
                  std::numeric_limits<std::uint32_t>::max(), false);
            } else if (read_key == "key_bytes_base64") {
              markField(read_seen, key_bytes_bit, read_key, "ClientTxn read");
              decoded_key = decodeBase64(
                  stringValue(field_value,
                              "ClientTxn read.key_bytes_base64"),
                  "ClientTxn read key", limits_.max_key_bytes);
            } else if (read_key == "value_state") {
              markField(read_seen, value_state_bit, read_key,
                        "ClientTxn read");
              state_text =
                  stringValue(field_value, "ClientTxn read.value_state");
            } else if (read_key == "value_bytes_base64") {
              markField(read_seen, value_bytes_bit, read_key,
                        "ClientTxn read");
              decoded_value = decodeBase64(
                  stringValue(field_value,
                              "ClientTxn read.value_bytes_base64"),
                  "ClientTxn read value", limits_.max_value_bytes);
            } else if (read_key == "source") {
              markField(read_seen, source_bit, read_key, "ClientTxn read");
              source_text =
                  stringValue(field_value, "ClientTxn read.source");
            } else {
              malformed("ClientTxn read contains unexpected field " +
                        std::string(read_key));
            }
          }

          const auto expected_fields = state_text == "present"
                                           ? present_fields
                                           : absent_fields;
          if (state_text != "present" && state_text != "absent") {
            malformed("ClientTxn read.value_state is invalid");
          }
          requireFields(read_seen, expected_fields, "ClientTxn read");
          if (event_seq != reads.size()) {
            malformed("ClientTxn read event_seq is not zero-based and dense");
          }
          if (!decoded_key.has_value() || decoded_key->empty()) {
            malformed("ClientTxn read key is empty");
          }
          const auto value_size = decoded_value.has_value()
                                      ? decoded_value->size()
                                      : 0U;
          if (decoded_key->size() > limits_.max_decoded_bytes ||
              value_size > limits_.max_decoded_bytes - decoded_key->size() ||
              decoded_bytes > limits_.max_decoded_bytes -
                                  decoded_key->size() - value_size) {
            throw MonitorError(MonitorErrorCode::ResourceExhausted,
                               "ClientTxn decoded reads exceed their byte "
                               "limit");
          }
          decoded_bytes += decoded_key->size() + value_size;
          benchmark_adapter_->validateClientRead(
              *decoded_key,
              decoded_value.has_value()
                  ? std::optional<std::span<const std::byte>>{*decoded_value}
                  : std::nullopt);

          ClientReadSource source;
          if (source_text == "snapshot") {
            source = ClientReadSource::Snapshot;
          } else if (source_text == "local") {
            source = ClientReadSource::Local;
          } else {
            throw MonitorError(MonitorErrorCode::UnsupportedReadSource,
                               "ClientTxn read source is unsupported");
          }
          reads.push_back(ClientRead{
              .event_seq = static_cast<std::uint32_t>(event_seq),
              .key = std::move(*decoded_key),
              .state = state_text == "present" ? ClientValueState::Present
                                                : ClientValueState::Absent,
              .value = std::move(decoded_value),
              .source = source,
          });
        }
      } else {
        malformed("ClientTxn contains unexpected field " + std::string(key));
      }
    }
    requireFields(seen, all_fields, "ClientTxn");

    if (kind != "client_txn") {
      malformed("ClientTxn.kind is not client_txn");
    }
    if (!isLowerUuid(run_id)) {
      malformed("ClientTxn.run_id is not a lowercase UUID");
    }
    if (run_id != run_id_) {
      throw MonitorError(MonitorErrorCode::RunIdMismatch,
                         "ClientTxn belongs to a different run_id");
    }
    if (!isSessionId(session_id)) {
      malformed("ClientTxn.session_id is invalid");
    }
    if (!expected_session_ids_.contains(session_id)) {
      throw MonitorError(MonitorErrorCode::SessionUnknown,
                         "ClientTxn names a session absent from the roster");
    }
    if (!isLowerUuid(logical_txn_id) || !isLowerUuid(attempt_id)) {
      malformed("ClientTxn logical/attempt identity is not a lowercase UUID");
    }
    if (!isProtocolName(txn_type)) {
      malformed("ClientTxn.txn_type is not a bounded protocol name");
    }
    if (!benchmark_adapter_->supportsTransaction(txn_type)) {
      throw MonitorError(MonitorErrorCode::UnsupportedTransaction,
                         "ClientTxn transaction type is not owned by the "
                         "selected benchmark adapter");
    }
    if (correlation_kind != "session_txn") {
      malformed("ClientTxn.correlation_kind is not session_txn");
    }
    if (!isLowerUuid(correlation_run) || !isSessionId(correlation_session)) {
      malformed("ClientTxn.correlation_id contains an invalid identity");
    }
    if (correlation_run != run_id || correlation_session != session_id) {
      throw MonitorError(
          MonitorErrorCode::CorrelationMismatch,
          "ClientTxn top-level and nested correlation identities disagree");
    }
    if (txn_id < session_seq) {
      throw MonitorError(
          MonitorErrorCode::SessionAttemptProgressInvalid,
          "ClientTxn marker txn_id is behind its logical session_seq");
    }

    if (reads.size() != read_count) {
      throw MonitorError(MonitorErrorCode::ClientReadCountMismatch,
                         "ClientTxn.read_count differs from reads length");
    }
    if (!isSha256(read_digest)) {
      malformed("ClientTxn.read_digest is not lowercase SHA-256 hex");
    }
    if (computeClientReadDigest(reads) != read_digest) {
      throw MonitorError(MonitorErrorCode::ClientReadDigestMismatch,
                         "ClientTxn read digest does not match its reads");
    }

    return ClientTxn{
        .run_id = run_id,
        .session_id = session_id,
        .session_seq = session_seq,
        .logical_txn_id = logical_txn_id,
        .attempt_id = attempt_id,
        .txn_type = txn_type,
        .correlation_id =
            RunScopedCorrelationId{
                .run_id = correlation_run,
                .session_id = correlation_session,
                .txn_id = txn_id,
            },
        .read_digest = read_digest,
        .reads = std::move(reads),
    };
  } catch (const MonitorError &) {
    throw;
  } catch (const std::bad_alloc &) {
    throw MonitorError(MonitorErrorCode::ResourceExhausted,
                       "allocation failed while parsing ClientTxn");
  } catch (const std::length_error &) {
    throw MonitorError(MonitorErrorCode::ResourceExhausted,
                       "container limit failed while parsing ClientTxn");
  } catch (const simdjson::simdjson_error &error) {
    const auto code = error.error();
    if (code == simdjson::MEMALLOC || code == simdjson::CAPACITY ||
        code == simdjson::DEPTH_ERROR) {
      throw MonitorError(MonitorErrorCode::ResourceExhausted,
                         std::string("ClientTxn JSON exceeds its limits: ") +
                             error.what());
    }
    throw MonitorError(MonitorErrorCode::ClientRecordMalformed,
                       std::string("invalid or ambiguous ClientTxn JSON: ") +
                           error.what());
  }
}

std::string_view toString(const ClientValueState state) noexcept {
  switch (state) {
  case ClientValueState::Present:
    return "present";
  case ClientValueState::Absent:
    return "absent";
  }
  std::abort();
}

std::string_view toString(const ClientReadSource source) noexcept {
  switch (source) {
  case ClientReadSource::Snapshot:
    return "snapshot";
  case ClientReadSource::Local:
    return "local";
  }
  std::abort();
}

} // namespace blackbox
