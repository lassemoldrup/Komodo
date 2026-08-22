#include "blackbox/protocol_records.h"

#include "blackbox/benchmark_adapter.h"

#include "strict_json.h"

#include <openssl/evp.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <initializer_list>
#include <limits>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <utility>

namespace blackbox {
namespace {

using Json = nlohmann::json;

constexpr std::uint64_t max_json_integer = 9007199254740991ULL;
constexpr char marker_seed_domain[] = "db-monitoring/session-marker-seed";
constexpr char marker_step_domain[] = "db-monitoring/session-marker-step";
constexpr char schema_attestation_domain[] = "db-monitoring/schema-attestation";

[[noreturn]] void malformed(const std::string &message) {
  throw MonitorError(MonitorErrorCode::ClientRecordMalformed, message);
}

[[noreturn]] void sessionMismatch(const std::string &message) {
  throw MonitorError(MonitorErrorCode::SessionDoneMismatch, message);
}

void requireObject(const Json &value, const std::string_view label) {
  if (!value.is_object()) {
    malformed(std::string(label) + " is not an object");
  }
}

void requireExactKeys(const Json &object,
                      const std::initializer_list<std::string_view> expected,
                      const std::string_view label) {
  requireObject(object, label);
  std::set<std::string, std::less<>> remaining;
  for (const auto key : expected) {
    remaining.emplace(key);
  }
  for (const auto &[key, unused] : object.items()) {
    (void)unused;
    if (remaining.erase(key) == 0U) {
      malformed(std::string(label) + " contains unexpected field " + key);
    }
  }
  if (!remaining.empty()) {
    malformed(std::string(label) + " is missing field " + *remaining.begin());
  }
}

[[nodiscard]] const Json &member(const Json &object, const std::string &name,
                                 const std::string_view label) {
  requireObject(object, label);
  const auto found = object.find(name);
  if (found == object.end()) {
    malformed(std::string(label) + " is missing field " + name);
  }
  return *found;
}

[[nodiscard]] std::string stringValue(const Json &value,
                                      const std::string_view label) {
  if (!value.is_string()) {
    malformed(std::string(label) + " is not a string");
  }
  return value.get<std::string>();
}

[[nodiscard]] std::uint64_t uintValue(const Json &value,
                                      const std::string_view label) {
  std::uint64_t result = 0;
  if (value.is_number_unsigned()) {
    result = value.get<std::uint64_t>();
  } else if (value.is_number_integer()) {
    const auto signed_value = value.get<std::int64_t>();
    if (signed_value < 0) {
      malformed(std::string(label) + " is negative");
    }
    result = static_cast<std::uint64_t>(signed_value);
  } else if (value.is_number_float()) {
    const auto floating_value = value.get<double>();
    if (!std::isfinite(floating_value) || floating_value < 0.0 ||
        std::trunc(floating_value) != floating_value ||
        floating_value > static_cast<double>(max_json_integer)) {
      malformed(std::string(label) + " is not a supported integer");
    }
    result = static_cast<std::uint64_t>(floating_value);
  } else {
    malformed(std::string(label) + " is not an integer");
  }
  if (result > max_json_integer) {
    malformed(std::string(label) + " exceeds the safe JSON integer range");
  }
  return result;
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

[[nodiscard]] bool isCanonicalIdentifier(const std::string_view value) {
  if (value.empty() || value.size() > 64U || value.front() < 'a' ||
      value.front() > 'z') {
    return false;
  }
  return std::all_of(value.begin() + 1, value.end(), [](const char character) {
    return (character >= 'a' && character <= 'z') ||
           (character >= '0' && character <= '9') || character == '_';
  });
}

[[nodiscard]] bool isCanonicalTable(const std::string_view value) {
  const auto separator = value.find('.');
  return separator != std::string_view::npos && separator != 0U &&
         separator + 1U < value.size() &&
         value.find('.', separator + 1U) == std::string_view::npos &&
         isCanonicalIdentifier(value.substr(0U, separator)) &&
         isCanonicalIdentifier(value.substr(separator + 1U));
}

[[nodiscard]] bool isSha256(const std::string_view value) {
  return value.size() == 64U &&
         std::all_of(value.begin(), value.end(), [](const char character) {
           return (character >= '0' && character <= '9') ||
                  (character >= 'a' && character <= 'f');
         });
}

[[nodiscard]] std::size_t utf8CodePoints(const std::string_view value) {
  return static_cast<std::size_t>(
      std::count_if(value.begin(), value.end(), [](const char character) {
        return (static_cast<unsigned char>(character) & 0xc0U) != 0x80U;
      }));
}

// All numeric fields in client-stream protocol records are counters. JSON
// Schema permits integral forms such as 1.0 and 1e0, but the DOM's binary64
// conversion could also round a fractional token to an integer. Inspect the
// original lexemes before conversion so only mathematically integral values
// reach uintValue().
void validateIntegralNumberLexemes(const std::string_view input) {
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
      malformed("protocol JSON contains an invalid number");
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
        malformed("protocol JSON contains an invalid fraction");
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
        malformed("protocol JSON contains an invalid exponent");
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
          malformed("protocol JSON contains a fractional counter");
        }
      }
    }
    cursor = number_cursor;
  }
}

template <typename Action>
[[nodiscard]] auto
translateFailures(const std::string_view label,
                  Action &&action) -> std::invoke_result_t<Action> {
  try {
    return std::forward<Action>(action)();
  } catch (const MonitorError &) {
    throw;
  } catch (const std::bad_alloc &) {
    throw MonitorError(MonitorErrorCode::ResourceExhausted,
                       "allocation failed while parsing " + std::string(label));
  } catch (const std::length_error &) {
    throw MonitorError(MonitorErrorCode::ResourceExhausted,
                       "container limit failed while parsing " +
                           std::string(label));
  } catch (const detail::StrictJsonLimitError &error) {
    throw MonitorError(MonitorErrorCode::ResourceExhausted,
                       std::string(label) +
                           " JSON exceeds its limits: " + error.what());
  } catch (const detail::StrictJsonError &error) {
    throw MonitorError(MonitorErrorCode::ClientRecordMalformed,
                       "invalid or ambiguous " + std::string(label) +
                           " JSON: " + error.what());
  } catch (const nlohmann::json::exception &error) {
    throw MonitorError(MonitorErrorCode::ClientRecordMalformed,
                       "failed to decode " + std::string(label) +
                           " JSON: " + error.what());
  }
}

[[nodiscard]] Json parseDocument(const std::string_view payload,
                                 const ProtocolRecordParserLimits &limits) {
  if (payload.empty()) {
    malformed("protocol record payload is empty");
  }
  if (payload.size() > limits.max_payload_bytes) {
    throw MonitorError(MonitorErrorCode::ResourceExhausted,
                       "protocol record payload exceeds its byte limit");
  }
  validateIntegralNumberLexemes(payload);
  return detail::parseStrictJson(
      payload, detail::StrictJsonLimits{
                   .max_depth = 8U,
                   .max_events = limits.max_payload_bytes + 1U,
                   .max_string_bytes = limits.max_payload_bytes,
                   .max_object_members = 16U,
               });
}

[[nodiscard]] std::string validatedUuid(const Json &value,
                                        const std::string_view label) {
  auto result = stringValue(value, label);
  if (!isLowerUuid(result)) {
    malformed(std::string(label) + " is not a lowercase UUID");
  }
  return result;
}

[[nodiscard]] std::string validatedSessionId(const Json &value,
                                             const std::string_view label) {
  auto result = stringValue(value, label);
  if (!isSessionId(result)) {
    malformed(std::string(label) + " is invalid");
  }
  return result;
}

[[nodiscard]] std::string validatedTable(const Json &value,
                                         const std::string_view label) {
  auto result = stringValue(value, label);
  if (!isCanonicalTable(result)) {
    malformed(std::string(label) + " is not a canonical table name");
  }
  return result;
}

[[nodiscard]] std::string validatedDigest(const Json &value,
                                          const std::string_view label) {
  auto result = stringValue(value, label);
  if (!isSha256(result)) {
    malformed(std::string(label) + " is not lowercase SHA-256 hex");
  }
  return result;
}

[[nodiscard]] std::vector<std::string>
parseStringArray(const Json &value, const std::string_view label,
                 const std::size_t limit, bool (*validator)(std::string_view)) {
  if (!value.is_array()) {
    malformed(std::string(label) + " is not an array");
  }
  if (value.empty()) {
    malformed(std::string(label) + " is empty");
  }
  if (value.size() > limit) {
    throw MonitorError(MonitorErrorCode::ResourceExhausted,
                       std::string(label) + " exceeds its configured limit");
  }
  std::vector<std::string> result;
  result.reserve(value.size());
  std::unordered_set<std::string> seen;
  seen.reserve(value.size());
  for (const auto &entry : value) {
    auto parsed = stringValue(entry, label);
    if (!validator(parsed)) {
      malformed(std::string(label) + " contains an invalid value");
    }
    if (!seen.insert(parsed).second) {
      malformed(std::string(label) + " contains a duplicate value");
    }
    result.push_back(std::move(parsed));
  }
  return result;
}

[[nodiscard]] bool allowedIsolation(const std::string_view value) {
  return value == "READ_COMMITTED" || value == "REPEATABLE_READ";
}

[[nodiscard]] bool allowedFailurePhase(const std::string_view value) {
  constexpr std::array<std::string_view, 9> phases = {
      "CONFIGURATION",     "CONNECTION_SETUP", "ATTEMPT_BODY",
      "ROLLBACK",          "COMMIT",           "MARKER_WRITE",
      "REDIS_PUBLICATION", "SESSION_SHUTDOWN", "COORDINATOR"};
  return std::find(phases.begin(), phases.end(), value) != phases.end();
}

[[nodiscard]] bool isErrorCodeSyntax(const std::string_view value) {
  if (value.empty() || value.size() > 96U || value.front() < 'A' ||
      value.front() > 'Z') {
    return false;
  }
  return std::all_of(value.begin() + 1, value.end(), [](const char character) {
    return (character >= 'A' && character <= 'Z') ||
           (character >= '0' && character <= '9') || character == '_';
  });
}

[[nodiscard]] bool documentedErrorCode(const std::string_view value) {
  constexpr std::string_view codes[] = {
      "CONFIG_INVALID",
      "BRANCH_CONTRACT_MISMATCH",
      "ISOLATION_MISMATCH",
      "SESSION_CONTRACT_MISMATCH",
      "UNSUPPORTED_TRANSACTION",
      "UNSUPPORTED_READ_SOURCE",
      "UNEXPECTED_TABLE",
      "DDL_OBSERVED",
      "EXTERNAL_WRITER_OBSERVED",
      "SCHEMA_ATTESTATION_MISSING",
      "SCHEMA_ATTESTATION_DUPLICATE",
      "SCHEMA_ATTESTATION_MALFORMED",
      "SCHEMA_ATTESTATION_MISMATCH",
      "SCHEMA_ATTESTATION_DRIFT",
      "BACKEND_FACT_MISMATCH",
      "CLIENT_RECORD_MALFORMED",
      "CLIENT_READ_COUNT_MISMATCH",
      "CLIENT_READ_DIGEST_MISMATCH",
      "CLIENT_DUPLICATE_CORRELATION",
      "LOGICAL_TRANSACTION_DUPLICATE",
      "ATTEMPT_DUPLICATE",
      "MULTIPLE_COMMITTED_ATTEMPTS",
      "SESSION_UNKNOWN",
      "SESSION_SEQUENCE_NON_INCREASING",
      "SESSION_ATTEMPT_PROGRESS_INVALID",
      "SESSION_DONE_MISSING",
      "SESSION_DONE_MISMATCH",
      "CLIENT_RECORD_AFTER_DONE",
      "COMMIT_OUTCOME_AMBIGUOUS",
      "MARKER_WRITE_FAILED",
      "REDIS_PUBLICATION_FAILED",
      "CDC_RECORD_MALFORMED",
      "CDC_BEGIN_MISSING",
      "CDC_END_MISSING",
      "CDC_TRANSACTION_ID_MISMATCH",
      "CDC_EVENT_ORDINAL_INVALID",
      "CDC_EVENT_COUNT_MISMATCH",
      "CDC_DUPLICATE_CONFLICT",
      "MARKER_MISSING",
      "MARKER_MULTIPLE",
      "MARKER_MALFORMED",
      "MARKER_NOT_LAST",
      "MARKER_BEFORE_IMAGE_MISSING",
      "MARKER_CHAIN_MISMATCH",
      "MARKER_DUPLICATE",
      "UNMARKED_APPLICATION_TRANSACTION",
      "CDC_APPLICATION_AFTER_END",
      "CDC_ORDER_INVALID",
      "CLIENT_HALF_MISSING",
      "CDC_HALF_MISSING",
      "CORRELATION_MISMATCH",
      "CANONICAL_ENCODING_FAILED",
      "PRIMARY_KEY_IMAGE_INCOMPLETE",
      "COMMIT_SEQUENCE_INVALID",
      "CHECKER_INPUT_REJECTED",
      "RUN_MANIFEST_MISSING",
      "RUN_MANIFEST_DUPLICATE",
      "RUN_ID_MISMATCH",
      "FENCE_MISSING",
      "FENCE_DUPLICATE",
      "FENCE_ORDER_INVALID",
      "BOOTSTRAP_MANIFEST_MISSING",
      "BOOTSTRAP_STATE_MISMATCH",
      "PHASE_ANCHOR_FAILED",
      "QUIESCENCE_UNPROVEN",
      "COMPONENT_RESTARTED",
      "RECONCILIATION_MISMATCH",
      "JOIN_BUFFER_LIMIT_EXCEEDED",
      "CHECKER_STATE_LIMIT_EXCEEDED",
      "RESOURCE_EXHAUSTED",
      "INTERNAL_ERROR",
  };
  return std::find(std::begin(codes), std::end(codes), value) !=
         std::end(codes);
}

class DigestContext final {
public:
  DigestContext()
      : context_(EVP_MD_CTX_new(),
                 [](EVP_MD_CTX *context) { EVP_MD_CTX_free(context); }) {
    if (!context_) {
      throw MonitorError(MonitorErrorCode::ResourceExhausted,
                         "could not allocate a SHA-256 marker digest context");
    }
    if (EVP_DigestInit_ex(context_.get(), EVP_sha256(), nullptr) != 1) {
      throw MonitorError(MonitorErrorCode::InternalError,
                         "could not initialize SHA-256 marker digest");
    }
  }

  void update(const void *data, const std::size_t size) {
    if (size != 0U && EVP_DigestUpdate(context_.get(), data, size) != 1) {
      throw MonitorError(MonitorErrorCode::InternalError,
                         "could not update SHA-256 marker digest");
    }
  }

  [[nodiscard]] SessionMarkerDigest finish() {
    std::array<unsigned char, EVP_MAX_MD_SIZE> bytes{};
    unsigned int size = 0;
    if (EVP_DigestFinal_ex(context_.get(), bytes.data(), &size) != 1 ||
        size != SessionMarkerDigest{}.size()) {
      throw MonitorError(MonitorErrorCode::InternalError,
                         "could not finish SHA-256 marker digest");
    }
    SessionMarkerDigest result{};
    std::copy_n(bytes.begin(), result.size(), result.begin());
    return result;
  }

private:
  std::unique_ptr<EVP_MD_CTX, void (*)(EVP_MD_CTX *)> context_;
};

[[nodiscard]] std::string digestHex(const void *prefix,
                                    const std::size_t prefix_size,
                                    const std::string_view body) {
  DigestContext digest;
  digest.update(prefix, prefix_size);
  digest.update(body.data(), body.size());
  const auto bytes = digest.finish();
  constexpr std::array<char, 16> hex = {'0', '1', '2', '3', '4', '5', '6', '7',
                                        '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  std::string result(bytes.size() * 2U, '0');
  for (std::size_t index = 0; index < bytes.size(); ++index) {
    result[index * 2U] = hex[bytes[index] >> 4U];
    result[index * 2U + 1U] = hex[bytes[index] & 0x0fU];
  }
  return result;
}

void updateU32(DigestContext &digest, const std::uint32_t value) {
  const std::array<unsigned char, 4> encoded = {
      static_cast<unsigned char>((value >> 24U) & 0xffU),
      static_cast<unsigned char>((value >> 16U) & 0xffU),
      static_cast<unsigned char>((value >> 8U) & 0xffU),
      static_cast<unsigned char>(value & 0xffU),
  };
  digest.update(encoded.data(), encoded.size());
}

void updateU64(DigestContext &digest, const std::uint64_t value) {
  std::array<unsigned char, 8> encoded{};
  for (std::size_t index = 0; index < encoded.size(); ++index) {
    const auto shift = static_cast<unsigned int>((7U - index) * 8U);
    encoded[index] = static_cast<unsigned char>((value >> shift) & 0xffU);
  }
  digest.update(encoded.data(), encoded.size());
}

[[nodiscard]] unsigned char hexNibble(const char character) {
  if (character >= '0' && character <= '9') {
    return static_cast<unsigned char>(character - '0');
  }
  return static_cast<unsigned char>(character - 'a' + 10);
}

[[nodiscard]] std::array<unsigned char, 16>
uuidBytes(const std::string_view run_id) {
  if (!isLowerUuid(run_id)) {
    throw MonitorError(MonitorErrorCode::ClientRecordMalformed,
                       "marker digest run_id is not a lowercase UUID");
  }
  std::array<unsigned char, 16> result{};
  std::size_t output = 0;
  for (std::size_t input = 0; input < run_id.size();) {
    if (run_id[input] == '-') {
      ++input;
      continue;
    }
    result[output] = static_cast<unsigned char>(
        (hexNibble(run_id[input]) << 4U) | hexNibble(run_id[input + 1U]));
    input += 2U;
    ++output;
  }
  return result;
}

} // namespace

ProtocolRecordParser::ProtocolRecordParser(
    const ProtocolRecordParserLimits limits)
    : limits_(limits) {
  if (limits_.max_payload_bytes == 0U ||
      limits_.max_payload_bytes >=
          std::numeric_limits<std::size_t>::max() - 1U ||
      limits_.max_expected_sessions == 0U || limits_.max_table_entries == 0U) {
    throw std::invalid_argument("protocol record parser limits are invalid");
  }
}

ProtocolRecordKind
ProtocolRecordParser::kind(const std::string_view payload) const {
  return translateFailures("protocol record", [&] {
    const Json record = parseDocument(payload, limits_);
    requireObject(record, "protocol record");
    const auto value = stringValue(member(record, "kind", "protocol record"),
                                   "protocol record.kind");
    if (value == "run_manifest") {
      return ProtocolRecordKind::RunManifest;
    }
    if (value == "schema_attestation") {
      return ProtocolRecordKind::SchemaAttestation;
    }
    if (value == "bootstrap_manifest") {
      return ProtocolRecordKind::BootstrapManifest;
    }
    if (value == "client_txn") {
      return ProtocolRecordKind::ClientTxn;
    }
    if (value == "session_done") {
      return ProtocolRecordKind::SessionDone;
    }
    if (value == "run_failed") {
      return ProtocolRecordKind::RunFailed;
    }
    malformed("protocol record.kind is unknown");
  });
}

RunManifest
ProtocolRecordParser::parseRunManifest(const std::string_view payload) const {
  return translateFailures("RunManifest", [&] {
    const Json record = parseDocument(payload, limits_);
    requireExactKeys(record,
                     {"kind", "run_id", "backend", "backend_source_id",
                      "declared_isolation", "benchmark", "expected_session_ids",
                      "application_table_allowlist", "transaction_marker_table",
                      "control_table_allowlist", "result_scope"},
                     "RunManifest");
    if (stringValue(member(record, "kind", "RunManifest"),
                    "RunManifest.kind") != "run_manifest") {
      malformed("RunManifest.kind is not run_manifest");
    }

    auto run_id = validatedUuid(member(record, "run_id", "RunManifest"),
                                "RunManifest.run_id");
    auto backend = stringValue(member(record, "backend", "RunManifest"),
                               "RunManifest.backend");
    if (!isProtocolName(backend)) {
      malformed("RunManifest.backend is not a bounded protocol name");
    }
    auto backend_source_id =
        stringValue(member(record, "backend_source_id", "RunManifest"),
                    "RunManifest.backend_source_id");
    const auto source_length = utf8CodePoints(backend_source_id);
    if (source_length == 0U || source_length > 256U) {
      malformed("RunManifest.backend_source_id length is outside 1..256");
    }
    auto isolation =
        stringValue(member(record, "declared_isolation", "RunManifest"),
                    "RunManifest.declared_isolation");
    if (!allowedIsolation(isolation)) {
      malformed("RunManifest.declared_isolation is unsupported");
    }
    auto benchmark = stringValue(member(record, "benchmark", "RunManifest"),
                                 "RunManifest.benchmark");
    if (!isProtocolName(benchmark)) {
      malformed("RunManifest.benchmark is not a bounded protocol name");
    }
    auto sessions =
        parseStringArray(member(record, "expected_session_ids", "RunManifest"),
                         "RunManifest.expected_session_ids",
                         limits_.max_expected_sessions, isSessionId);
    auto application_tables = parseStringArray(
        member(record, "application_table_allowlist", "RunManifest"),
        "RunManifest.application_table_allowlist", limits_.max_table_entries,
        isCanonicalTable);
    auto marker_table = validatedTable(
        member(record, "transaction_marker_table", "RunManifest"),
        "RunManifest.transaction_marker_table");
    auto control_tables = parseStringArray(
        member(record, "control_table_allowlist", "RunManifest"),
        "RunManifest.control_table_allowlist", limits_.max_table_entries,
        isCanonicalTable);
    auto result_scope =
        stringValue(member(record, "result_scope", "RunManifest"),
                    "RunManifest.result_scope");
    if (result_scope != "point_transaction_soww") {
      malformed("RunManifest.result_scope is unsupported");
    }

    const std::unordered_set<std::string> application_set(
        application_tables.begin(), application_tables.end());
    const std::unordered_set<std::string> control_set(control_tables.begin(),
                                                      control_tables.end());
    if (application_set.contains(marker_table) ||
        control_set.contains(marker_table)) {
      malformed("RunManifest marker table overlaps an allowlist");
    }
    for (const auto &table : application_set) {
      if (control_set.contains(table)) {
        malformed("RunManifest application/control allowlists overlap");
      }
    }

    return RunManifest{
        .run_id = std::move(run_id),
        .backend = std::move(backend),
        .backend_source_id = std::move(backend_source_id),
        .declared_isolation = std::move(isolation),
        .benchmark = std::move(benchmark),
        .expected_session_ids = std::move(sessions),
        .application_table_allowlist = std::move(application_tables),
        .transaction_marker_table = std::move(marker_table),
        .control_table_allowlist = std::move(control_tables),
        .result_scope = std::move(result_scope),
    };
  });
}

SchemaAttestation ProtocolRecordParser::parseSchemaAttestation(
    const std::string_view payload) const {
  return translateFailures("SchemaAttestation", [&] {
    const Json record = parseDocument(payload, limits_);
    requireExactKeys(record,
                     {"kind", "run_id", "phase", "backend",
                      "backend_source_id", "tables", "backend_facts",
                      "canonical_attestation_digest"},
                     "SchemaAttestation");
    if (stringValue(member(record, "kind", "SchemaAttestation"),
                    "SchemaAttestation.kind") != "schema_attestation") {
      malformed("SchemaAttestation.kind is not schema_attestation");
    }
    auto run_id = validatedUuid(member(record, "run_id", "SchemaAttestation"),
                                "SchemaAttestation.run_id");
    const auto phase_text = stringValue(
        member(record, "phase", "SchemaAttestation"),
        "SchemaAttestation.phase");
    SchemaAttestationPhase phase;
    if (phase_text == "STARTUP") {
      phase = SchemaAttestationPhase::Startup;
    } else if (phase_text == "FINAL") {
      phase = SchemaAttestationPhase::Final;
    } else {
      malformed("SchemaAttestation.phase is invalid");
    }
    auto backend = stringValue(member(record, "backend", "SchemaAttestation"),
                               "SchemaAttestation.backend");
    if (!isProtocolName(backend)) {
      malformed("SchemaAttestation.backend is not a bounded protocol name");
    }
    auto backend_source_id = stringValue(
        member(record, "backend_source_id", "SchemaAttestation"),
        "SchemaAttestation.backend_source_id");
    const auto source_length = utf8CodePoints(backend_source_id);
    if (source_length == 0U || source_length > 256U) {
      malformed("SchemaAttestation.backend_source_id length is outside 1..256");
    }

    const auto &table_values = record.at("tables");
    if (!table_values.is_array() || table_values.size() < 3U) {
      malformed("SchemaAttestation.tables is not a complete table roster");
    }
    if (table_values.size() > limits_.max_table_entries) {
      throw MonitorError(MonitorErrorCode::ResourceExhausted,
                         "SchemaAttestation table roster exceeds its limit");
    }
    std::vector<ProtocolSchemaTable> tables;
    tables.reserve(table_values.size());
    std::string previous_table;
    for (const auto &table_value : table_values) {
      requireExactKeys(table_value,
                       {"canonical_table_name", "primary_key_columns",
                        "columns"},
                       "SchemaAttestation table");
      auto table_name = validatedTable(
          member(table_value, "canonical_table_name", "SchemaAttestation table"),
          "SchemaAttestation table.canonical_table_name");
      if (!previous_table.empty() && table_name <= previous_table) {
        malformed("SchemaAttestation tables are duplicated or not sorted");
      }
      previous_table = table_name;
      auto primary_keys = parseStringArray(
          member(table_value, "primary_key_columns", "SchemaAttestation table"),
          "SchemaAttestation table.primary_key_columns", limits_.max_table_entries,
          isCanonicalIdentifier);
      const auto &column_values = table_value.at("columns");
      if (!column_values.is_array() || column_values.empty()) {
        malformed("SchemaAttestation table.columns is empty or malformed");
      }
      if (column_values.size() > limits_.max_table_entries) {
        throw MonitorError(MonitorErrorCode::ResourceExhausted,
                           "SchemaAttestation column roster exceeds its limit");
      }
      std::vector<ProtocolSchemaColumn> columns;
      columns.reserve(column_values.size());
      std::unordered_set<std::string> column_names;
      for (const auto &column_value : column_values) {
        requireObject(column_value, "SchemaAttestation column");
        if (column_value.size() == 3U) {
          requireExactKeys(column_value,
                           {"name", "logical_sql_type", "nullable"},
                           "SchemaAttestation column");
        } else {
          requireExactKeys(column_value,
                           {"name", "logical_sql_type", "nullable", "generated"},
                           "SchemaAttestation column");
        }
        auto name = stringValue(
            member(column_value, "name", "SchemaAttestation column"),
            "SchemaAttestation column.name");
        if (!isCanonicalIdentifier(name) || !column_names.insert(name).second) {
          malformed("SchemaAttestation column name is invalid or duplicated");
        }
        auto logical_type = stringValue(
            member(column_value, "logical_sql_type", "SchemaAttestation column"),
            "SchemaAttestation column.logical_sql_type");
        const auto logical_length = utf8CodePoints(logical_type);
        if (logical_length == 0U || logical_length > 128U) {
          malformed("SchemaAttestation logical SQL type length is invalid");
        }
        const auto &nullable_value = column_value.at("nullable");
        if (!nullable_value.is_boolean()) {
          malformed("SchemaAttestation column.nullable is not boolean");
        }
        bool generated = false;
        if (const auto found = column_value.find("generated");
            found != column_value.end()) {
          if (!found->is_boolean()) {
            malformed("SchemaAttestation column.generated is not boolean");
          }
          generated = found->get<bool>();
          if (!generated) {
            malformed("SchemaAttestation column.generated may only be present as true");
          }
        }
        columns.push_back(ProtocolSchemaColumn{
            .name = std::move(name),
            .logical_sql_type = std::move(logical_type),
            .nullable = nullable_value.get<bool>(),
            .generated = generated,
        });
      }
      for (const auto &primary_key : primary_keys) {
        if (!column_names.contains(primary_key)) {
          malformed("SchemaAttestation primary key names an unknown column");
        }
      }
      tables.push_back(ProtocolSchemaTable{
          .canonical_table_name = std::move(table_name),
          .primary_key_columns = std::move(primary_keys),
          .columns = std::move(columns),
      });
    }

    const auto &fact_values = record.at("backend_facts");
    if (!fact_values.is_array() || fact_values.empty()) {
      malformed("SchemaAttestation.backend_facts is empty or malformed");
    }
    if (fact_values.size() > limits_.max_table_entries) {
      throw MonitorError(MonitorErrorCode::ResourceExhausted,
                         "SchemaAttestation backend fact roster exceeds its limit");
    }
    std::vector<ProtocolBackendFact> facts;
    facts.reserve(fact_values.size());
    std::unordered_set<std::string> fact_names;
    for (const auto &fact_value : fact_values) {
      requireExactKeys(fact_value, {"name", "canonical_value"},
                       "SchemaAttestation backend fact");
      auto name = stringValue(
          member(fact_value, "name", "SchemaAttestation backend fact"),
          "SchemaAttestation backend fact.name");
      if (!isCanonicalIdentifier(name) || !fact_names.insert(name).second) {
        malformed("SchemaAttestation backend fact name is invalid or duplicated");
      }
      auto value = stringValue(
          member(fact_value, "canonical_value", "SchemaAttestation backend fact"),
          "SchemaAttestation backend fact.canonical_value");
      if (utf8CodePoints(value) > 4096U) {
        malformed("SchemaAttestation backend fact value is oversized");
      }
      facts.push_back(ProtocolBackendFact{.name = std::move(name),
                                          .canonical_value = std::move(value)});
    }
    auto attestation_digest = validatedDigest(
        member(record, "canonical_attestation_digest", "SchemaAttestation"),
        "SchemaAttestation.canonical_attestation_digest");
    const Json digest_body = {
        {"backend", backend},
        {"backend_source_id", backend_source_id},
        {"tables", table_values},
        {"backend_facts", fact_values},
    };
    const auto expected_digest = digestHex(
        schema_attestation_domain, sizeof(schema_attestation_domain),
        digest_body.dump(-1, ' ', false, Json::error_handler_t::strict));
    if (attestation_digest != expected_digest) {
      malformed("SchemaAttestation canonical digest differs");
    }
    return SchemaAttestation{
        .run_id = std::move(run_id),
        .phase = phase,
        .backend = std::move(backend),
        .backend_source_id = std::move(backend_source_id),
        .tables = std::move(tables),
        .backend_facts = std::move(facts),
        .canonical_attestation_digest = std::move(attestation_digest),
    };
  });
}

BootstrapManifest ProtocolRecordParser::parseBootstrapManifest(
    const std::string_view payload) const {
  return translateFailures("BootstrapManifest", [&] {
    const Json record = parseDocument(payload, limits_);
    requireExactKeys(
        record,
        {"kind", "run_id", "table_row_counts", "canonical_state_digest"},
        "BootstrapManifest");
    if (stringValue(member(record, "kind", "BootstrapManifest"),
                    "BootstrapManifest.kind") != "bootstrap_manifest") {
      malformed("BootstrapManifest.kind is not bootstrap_manifest");
    }
    auto run_id = validatedUuid(member(record, "run_id", "BootstrapManifest"),
                                "BootstrapManifest.run_id");
    const Json *const counts =
        &member(record, "table_row_counts", "BootstrapManifest");
    if (!counts->is_array()) {
      malformed("BootstrapManifest.table_row_counts is not an array");
    }
    if (counts->empty()) {
      malformed("BootstrapManifest.table_row_counts is empty");
    }
    if (counts->size() > limits_.max_table_entries) {
      throw MonitorError(
          MonitorErrorCode::ResourceExhausted,
          "BootstrapManifest.table_row_counts exceeds its configured limit");
    }

    std::vector<ProtocolTableRowCount> parsed_counts;
    parsed_counts.reserve(counts->size());
    std::string previous_table;
    for (const auto &entry : *counts) {
      requireExactKeys(entry, {"table", "row_count"},
                       "BootstrapManifest table count");
      auto table = validatedTable(
          member(entry, "table", "BootstrapManifest table count"),
          "BootstrapManifest table count.table");
      if (!previous_table.empty() && table <= previous_table) {
        malformed("BootstrapManifest table counts are duplicated or unsorted");
      }
      previous_table = table;
      parsed_counts.push_back(ProtocolTableRowCount{
          .table = std::move(table),
          .row_count = uintValue(
              member(entry, "row_count", "BootstrapManifest table count"),
              "BootstrapManifest table count.row_count"),
      });
    }
    auto digest = validatedDigest(
        member(record, "canonical_state_digest", "BootstrapManifest"),
        "BootstrapManifest.canonical_state_digest");
    return BootstrapManifest{
        .run_id = std::move(run_id),
        .table_row_counts = std::move(parsed_counts),
        .canonical_state_digest = std::move(digest),
    };
  });
}

SessionDone
ProtocolRecordParser::parseSessionDone(const std::string_view payload) const {
  return translateFailures("SessionDone", [&] {
    const Json record = parseDocument(payload, limits_);
    requireExactKeys(
        record,
        {"kind", "run_id", "session_id", "final_session_seq",
         "logical_transactions_started", "successful_transactions",
         "terminal_aborted_transactions", "physical_attempts_started",
         "definitively_aborted_attempts", "published_transactions",
         "retry_attempts", "ambiguous_attempts", "rolling_correlation_digest"},
        "SessionDone");
    if (stringValue(member(record, "kind", "SessionDone"),
                    "SessionDone.kind") != "session_done") {
      malformed("SessionDone.kind is not session_done");
    }

    SessionDone result{
        .run_id = validatedUuid(member(record, "run_id", "SessionDone"),
                                "SessionDone.run_id"),
        .session_id =
            validatedSessionId(member(record, "session_id", "SessionDone"),
                               "SessionDone.session_id"),
        .final_session_seq =
            uintValue(member(record, "final_session_seq", "SessionDone"),
                      "SessionDone.final_session_seq"),
        .logical_transactions_started = uintValue(
            member(record, "logical_transactions_started", "SessionDone"),
            "SessionDone.logical_transactions_started"),
        .successful_transactions =
            uintValue(member(record, "successful_transactions", "SessionDone"),
                      "SessionDone.successful_transactions"),
        .terminal_aborted_transactions = uintValue(
            member(record, "terminal_aborted_transactions", "SessionDone"),
            "SessionDone.terminal_aborted_transactions"),
        .physical_attempts_started = uintValue(
            member(record, "physical_attempts_started", "SessionDone"),
            "SessionDone.physical_attempts_started"),
        .definitively_aborted_attempts = uintValue(
            member(record, "definitively_aborted_attempts", "SessionDone"),
            "SessionDone.definitively_aborted_attempts"),
        .published_transactions =
            uintValue(member(record, "published_transactions", "SessionDone"),
                      "SessionDone.published_transactions"),
        .retry_attempts =
            uintValue(member(record, "retry_attempts", "SessionDone"),
                      "SessionDone.retry_attempts"),
        .ambiguous_attempts =
            uintValue(member(record, "ambiguous_attempts", "SessionDone"),
                      "SessionDone.ambiguous_attempts"),
        .rolling_correlation_digest = validatedDigest(
            member(record, "rolling_correlation_digest", "SessionDone"),
            "SessionDone.rolling_correlation_digest"),
    };

    if (result.final_session_seq != result.logical_transactions_started) {
      sessionMismatch(
          "SessionDone final/logical transaction counters disagree");
    }
    if (result.logical_transactions_started !=
        result.successful_transactions + result.terminal_aborted_transactions) {
      sessionMismatch(
          "SessionDone logical transaction counters do not reconcile");
    }
    if (result.successful_transactions != result.published_transactions) {
      sessionMismatch("SessionDone successful/published counters disagree");
    }
    if (result.physical_attempts_started !=
        result.successful_transactions + result.definitively_aborted_attempts +
            result.ambiguous_attempts) {
      sessionMismatch("SessionDone physical attempt counters do not reconcile");
    }
    if (result.physical_attempts_started <
            result.logical_transactions_started ||
        result.retry_attempts != result.physical_attempts_started -
                                     result.logical_transactions_started) {
      sessionMismatch("SessionDone retry counter does not reconcile");
    }
    if (result.ambiguous_attempts != 0U) {
      sessionMismatch("SessionDone reports an ambiguous attempt");
    }
    return result;
  });
}

RunFailed
ProtocolRecordParser::parseRunFailed(const std::string_view payload) const {
  return translateFailures("RunFailed", [&] {
    const Json record = parseDocument(payload, limits_);
    const bool has_session =
        record.is_object() && record.contains("session_id");
    if (has_session) {
      requireExactKeys(record,
                       {"kind", "run_id", "session_id", "phase", "reason_code",
                        "diagnostic"},
                       "RunFailed");
    } else {
      requireExactKeys(record,
                       {"kind", "run_id", "phase", "reason_code", "diagnostic"},
                       "RunFailed");
    }
    if (stringValue(member(record, "kind", "RunFailed"), "RunFailed.kind") !=
        "run_failed") {
      malformed("RunFailed.kind is not run_failed");
    }
    auto run_id = validatedUuid(member(record, "run_id", "RunFailed"),
                                "RunFailed.run_id");
    std::optional<std::string> session_id;
    if (has_session) {
      session_id = validatedSessionId(member(record, "session_id", "RunFailed"),
                                      "RunFailed.session_id");
    }
    auto phase =
        stringValue(member(record, "phase", "RunFailed"), "RunFailed.phase");
    if (!allowedFailurePhase(phase)) {
      malformed("RunFailed.phase is unsupported");
    }
    auto reason = stringValue(member(record, "reason_code", "RunFailed"),
                              "RunFailed.reason_code");
    if (!isErrorCodeSyntax(reason) || !documentedErrorCode(reason)) {
      malformed("RunFailed.reason_code is not a documented error code");
    }
    auto diagnostic = stringValue(member(record, "diagnostic", "RunFailed"),
                                  "RunFailed.diagnostic");
    const auto diagnostic_length = utf8CodePoints(diagnostic);
    if (diagnostic_length == 0U || diagnostic_length > 4096U) {
      malformed("RunFailed.diagnostic length is outside 1..4096");
    }
    return RunFailed{
        .run_id = std::move(run_id),
        .session_id = std::move(session_id),
        .phase = std::move(phase),
        .reason_code = std::move(reason),
        .diagnostic = std::move(diagnostic),
    };
  });
}

SessionMarkerDigest sessionMarkerDigestSeed(const std::string_view run_id,
                                            const std::string_view session_id) {
  if (!isSessionId(session_id)) {
    throw MonitorError(MonitorErrorCode::ClientRecordMalformed,
                       "marker digest session_id is invalid");
  }
  const auto run_bytes = uuidBytes(run_id);
  DigestContext digest;
  digest.update(marker_seed_domain, sizeof(marker_seed_domain));
  digest.update(run_bytes.data(), run_bytes.size());
  updateU32(digest, static_cast<std::uint32_t>(session_id.size()));
  digest.update(session_id.data(), session_id.size());
  return digest.finish();
}

SessionMarkerDigest sessionMarkerDigestStep(const SessionMarkerDigest &previous,
                                            const SessionMarkerEntry entry) {
  if (entry.session_seq == 0U || entry.session_seq > max_json_integer ||
      entry.txn_id == 0U || entry.txn_id > max_json_integer) {
    throw MonitorError(
        MonitorErrorCode::ClientRecordMalformed,
        "marker digest entry is outside the positive safe range");
  }
  DigestContext digest;
  digest.update(marker_step_domain, sizeof(marker_step_domain));
  digest.update(previous.data(), previous.size());
  updateU64(digest, entry.session_seq);
  updateU64(digest, entry.txn_id);
  return digest.finish();
}

std::string sessionMarkerDigestHex(const SessionMarkerDigest &digest) {
  constexpr std::array<char, 16> hex = {'0', '1', '2', '3', '4', '5', '6', '7',
                                        '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  std::string result(digest.size() * 2U, '0');
  for (std::size_t index = 0; index < digest.size(); ++index) {
    result[index * 2U] = hex[digest[index] >> 4U];
    result[index * 2U + 1U] = hex[digest[index] & 0x0fU];
  }
  return result;
}

std::string computeRollingSessionMarkerDigest(
    const std::string_view run_id, const std::string_view session_id,
    const std::span<const SessionMarkerEntry> entries) {
  auto digest = sessionMarkerDigestSeed(run_id, session_id);
  for (const auto entry : entries) {
    digest = sessionMarkerDigestStep(digest, entry);
  }
  return sessionMarkerDigestHex(digest);
}

std::string
computeSchemaAttestationDigest(const SchemaAttestation &attestation) {
  Json tables = Json::array();
  for (const auto &table : attestation.tables) {
    Json columns = Json::array();
    for (const auto &column : table.columns) {
      Json encoded{{"name", column.name},
                   {"logical_sql_type", column.logical_sql_type},
                   {"nullable", column.nullable}};
      if (column.generated) {
        encoded["generated"] = true;
      }
      columns.push_back(std::move(encoded));
    }
    tables.push_back(Json{
        {"canonical_table_name", table.canonical_table_name},
        {"primary_key_columns", table.primary_key_columns},
        {"columns", std::move(columns)},
    });
  }
  Json facts = Json::array();
  for (const auto &fact : attestation.backend_facts) {
    facts.push_back(
        Json{{"name", fact.name}, {"canonical_value", fact.canonical_value}});
  }
  const Json body = {
      {"backend", attestation.backend},
      {"backend_source_id", attestation.backend_source_id},
      {"tables", std::move(tables)},
      {"backend_facts", std::move(facts)},
  };
  return digestHex(schema_attestation_domain, sizeof(schema_attestation_domain),
                   body.dump(-1, ' ', false, Json::error_handler_t::strict));
}

std::string_view toString(const ProtocolRecordKind kind) noexcept {
  switch (kind) {
  case ProtocolRecordKind::RunManifest:
    return "run_manifest";
  case ProtocolRecordKind::SchemaAttestation:
    return "schema_attestation";
  case ProtocolRecordKind::BootstrapManifest:
    return "bootstrap_manifest";
  case ProtocolRecordKind::ClientTxn:
    return "client_txn";
  case ProtocolRecordKind::SessionDone:
    return "session_done";
  case ProtocolRecordKind::RunFailed:
    return "run_failed";
  }
  std::abort();
}

std::string_view toString(const SchemaAttestationPhase phase) noexcept {
  switch (phase) {
  case SchemaAttestationPhase::Startup:
    return "STARTUP";
  case SchemaAttestationPhase::Final:
    return "FINAL";
  }
  std::abort();
}

} // namespace blackbox
