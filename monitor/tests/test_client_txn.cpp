#include "blackbox/client_txn.h"

#include "test_support.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {

using blackbox::CanonicalBytes;
using blackbox::CanonicalYcsbCodec;
using blackbox::ClientRead;
using blackbox::ClientReadSource;
using blackbox::ClientTxnParser;
using blackbox::ClientTxnParserLimits;
using blackbox::ClientValueState;
using blackbox::MonitorError;
using blackbox::MonitorErrorCode;
using Json = nlohmann::json;

constexpr std::string_view run_id = "00112233-4455-6677-8899-aabbccddeeff";
constexpr std::string_view session_id = "ycsb/worker/1";

[[nodiscard]] std::string readFile(const std::filesystem::path &path) {
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    throw std::runtime_error("could not open test input " + path.string());
  }
  std::ostringstream contents;
  contents << input.rdbuf();
  return contents.str();
}

[[nodiscard]] ClientTxnParser parser() {
  return ClientTxnParser(std::string(run_id),
                         {std::string(session_id), "ycsb/worker/2"},
                         {"benchbase.usertable"});
}

[[nodiscard]] std::string encodeBase64(const std::span<const std::byte> bytes) {
  constexpr std::string_view alphabet =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string result;
  result.reserve((bytes.size() + 2U) / 3U * 4U);
  for (std::size_t index = 0; index < bytes.size(); index += 3U) {
    const auto first = std::to_integer<unsigned int>(bytes[index]);
    const bool has_second = index + 1U < bytes.size();
    const bool has_third = index + 2U < bytes.size();
    const auto second =
        has_second ? std::to_integer<unsigned int>(bytes[index + 1U]) : 0U;
    const auto third =
        has_third ? std::to_integer<unsigned int>(bytes[index + 2U]) : 0U;
    result.push_back(alphabet[(first >> 2U) & 0x3fU]);
    result.push_back(
        alphabet[((first & 0x03U) << 4U) | ((second >> 4U) & 0x0fU)]);
    result.push_back(
        has_second
            ? alphabet[((second & 0x0fU) << 2U) | ((third >> 6U) & 0x03U)]
            : '=');
    result.push_back(has_third ? alphabet[third & 0x3fU] : '=');
  }
  return result;
}

[[nodiscard]] Json readJson(const ClientRead &read) {
  Json result{{"event_seq", read.event_seq},
              {"key_bytes_base64", encodeBase64(read.key)},
              {"value_state",
               read.state == ClientValueState::Present ? "present" : "absent"},
              {"source", read.source == ClientReadSource::Snapshot ? "snapshot"
                                                                   : "local"}};
  if (read.value.has_value()) {
    result["value_bytes_base64"] = encodeBase64(*read.value);
  }
  return result;
}

[[nodiscard]] Json recordWithReads(const std::vector<ClientRead> &reads) {
  auto record = Json::parse(
      readFile(std::filesystem::path(BLACKBOX_PROTOCOL_EXAMPLE_DIR) /
               "client-txn-second.json"));
  record["session_seq"] = 1;
  record["correlation_id"]["txn_id"] = 1;
  record["reads"] = Json::array();
  for (const auto &read : reads) {
    record["reads"].push_back(readJson(read));
  }
  record["read_count"] = reads.size();
  record["read_digest"] = blackbox::computeClientReadDigest(reads);
  return record;
}

[[nodiscard]] CanonicalBytes
valueWithFields(const std::array<std::optional<std::string>, 10> &fields) {
  return CanonicalYcsbCodec::encodeValue(fields);
}

void expectError(const ClientTxnParser &client_parser,
                 const std::string_view payload,
                 const MonitorErrorCode expected) {
  try {
    (void)client_parser.parse(payload);
    BLACKBOX_CHECK(false);
  } catch (const MonitorError &error) {
    BLACKBOX_CHECK(error.code() == expected);
  }
}

void testGoldenRecords() {
  const auto mixed_payload = readFile(
      std::filesystem::path(BLACKBOX_PROTOCOL_EXAMPLE_DIR) / "client-txn.json");
  const auto mixed = parser().parse(mixed_payload);
  BLACKBOX_CHECK(mixed.run_id == run_id);
  BLACKBOX_CHECK(mixed.session_id == session_id);
  BLACKBOX_CHECK(mixed.session_seq == 1U);
  BLACKBOX_CHECK(mixed.logical_txn_id ==
                 "11111111-1111-4111-8111-111111111111");
  BLACKBOX_CHECK(mixed.attempt_id == "22222222-2222-4222-8222-222222222222");
  BLACKBOX_CHECK(mixed.txn_type == "si_fractured_read_probe");
  BLACKBOX_CHECK(mixed.correlation_id.run_id == run_id);
  BLACKBOX_CHECK(mixed.correlation_id.session_id == session_id);
  BLACKBOX_CHECK(mixed.correlation_id.txn_id == 1U);
  BLACKBOX_CHECK(mixed.reads.size() == 2U);
  BLACKBOX_CHECK(mixed.reads[0].event_seq == 0U);
  BLACKBOX_CHECK(mixed.reads[0].state == ClientValueState::Present);
  BLACKBOX_CHECK(mixed.reads[0].source == ClientReadSource::Snapshot);
  BLACKBOX_CHECK(mixed.reads[0].value.has_value());
  BLACKBOX_CHECK(mixed.reads[0].value->size() == 69U);
  BLACKBOX_CHECK(encodeBase64(mixed.reads[0].key) ==
                 "AAAACWJlbmNoYmFzZQAAAAl1c2VydGFibGUAAAAq");
  BLACKBOX_CHECK(encodeBase64(*mixed.reads[0].value) ==
                 "AAEAAAAAAQAAAAFBAQAAAALDqQEAAAAD5LitAQAAAAEwAQAAAAVo"
                 "ZWxsbwEAAAAE8J+ZggEAAAADYQBiAQAAAARsYXN0");
  BLACKBOX_CHECK(mixed.reads[1].event_seq == 1U);
  BLACKBOX_CHECK(mixed.reads[1].state == ClientValueState::Absent);
  BLACKBOX_CHECK(mixed.reads[1].source == ClientReadSource::Local);
  BLACKBOX_CHECK(!mixed.reads[1].value.has_value());

  const auto empty_payload =
      readFile(std::filesystem::path(BLACKBOX_PROTOCOL_EXAMPLE_DIR) /
               "client-txn-second.json");
  const auto empty = parser().parse(empty_payload);
  BLACKBOX_CHECK(empty.session_seq == 3U);
  BLACKBOX_CHECK(empty.correlation_id.txn_id == 3U);
  BLACKBOX_CHECK(empty.reads.empty());

  const auto golden =
      Json::parse(readFile(std::filesystem::path(BLACKBOX_PROTOCOL_GOLDEN_DIR) /
                           "read_digests.json"));
  for (const auto &vector : golden.at("vectors")) {
    auto record = Json::parse(empty_payload);
    record["reads"] = vector.at("reads");
    record["read_count"] = vector.at("reads").size();
    record["read_digest"] = vector.at("digest");
    const auto parsed = parser().parse(record.dump());
    BLACKBOX_CHECK(blackbox::computeClientReadDigest(parsed.reads) ==
                   vector.at("digest").get<std::string>());
  }
}

void testMalformedStructure() {
  const auto path =
      std::filesystem::path(BLACKBOX_PROTOCOL_EXAMPLE_DIR) / "client-txn.json";
  const auto payload = readFile(path);
  const auto client_parser = parser();

  expectError(client_parser, "not-json",
              MonitorErrorCode::ClientRecordMalformed);

  auto compact = Json::parse(payload).dump();
  const auto kind_position = compact.find("\"kind\":\"client_txn\"");
  BLACKBOX_REQUIRE(kind_position != std::string::npos);
  compact.insert(kind_position, "\"kind\":\"client_txn\",");
  expectError(client_parser, compact, MonitorErrorCode::ClientRecordMalformed);

  compact = Json::parse(payload).dump();
  const auto txn_position = compact.find("\"txn_id\":1");
  BLACKBOX_REQUIRE(txn_position != std::string::npos);
  compact.insert(txn_position, "\"txn_id\":1,");
  expectError(client_parser, compact, MonitorErrorCode::ClientRecordMalformed);

  compact = Json::parse(payload).dump();
  const auto event_position = compact.find("\"event_seq\":0");
  BLACKBOX_REQUIRE(event_position != std::string::npos);
  compact.insert(event_position, "\"event_seq\":0,");
  expectError(client_parser, compact, MonitorErrorCode::ClientRecordMalformed);

  auto record = Json::parse(payload);
  record["extra"] = true;
  expectError(client_parser, record.dump(),
              MonitorErrorCode::ClientRecordMalformed);
  record = Json::parse(payload);
  record.erase("attempt_id");
  expectError(client_parser, record.dump(),
              MonitorErrorCode::ClientRecordMalformed);
  record = Json::parse(payload);
  record["logical_txn_id"] = "11111111-1111-4111-8111-11111111111A";
  expectError(client_parser, record.dump(),
              MonitorErrorCode::ClientRecordMalformed);
  record = Json::parse(payload);
  record["session_seq"] = 0;
  expectError(client_parser, record.dump(),
              MonitorErrorCode::ClientRecordMalformed);
  record = Json::parse(payload);
  record["txn_type"] = "arbitrary_sql";
  expectError(client_parser, record.dump(),
              MonitorErrorCode::UnsupportedTransaction);
  record = Json::parse(payload);
  record["correlation_kind"] = "server_transaction";
  expectError(client_parser, record.dump(),
              MonitorErrorCode::ClientRecordMalformed);
}

void testRunSessionAndCorrelation() {
  const auto payload = readFile(
      std::filesystem::path(BLACKBOX_PROTOCOL_EXAMPLE_DIR) / "client-txn.json");
  const auto client_parser = parser();
  auto record = Json::parse(payload);
  record["run_id"] = "10112233-4455-6677-8899-aabbccddeeff";
  record["correlation_id"]["run_id"] = record["run_id"];
  expectError(client_parser, record.dump(), MonitorErrorCode::RunIdMismatch);
  record = Json::parse(payload);
  record["session_id"] = "ycsb/worker/unknown";
  record["correlation_id"]["session_id"] = record["session_id"];
  expectError(client_parser, record.dump(), MonitorErrorCode::SessionUnknown);
  record = Json::parse(payload);
  record["correlation_id"]["session_id"] = "ycsb/worker/2";
  expectError(client_parser, record.dump(),
              MonitorErrorCode::CorrelationMismatch);
  record = Json::parse(payload);
  record["correlation_id"]["run_id"] = "10112233-4455-6677-8899-aabbccddeeff";
  expectError(client_parser, record.dump(),
              MonitorErrorCode::CorrelationMismatch);
}

void testReadValidation() {
  const auto payload = readFile(
      std::filesystem::path(BLACKBOX_PROTOCOL_EXAMPLE_DIR) / "client-txn.json");
  const auto client_parser = parser();
  auto record = Json::parse(payload);
  record["read_count"] = 1;
  expectError(client_parser, record.dump(),
              MonitorErrorCode::ClientReadCountMismatch);
  record = Json::parse(payload);
  record["reads"][0]["event_seq"] = 1;
  expectError(client_parser, record.dump(),
              MonitorErrorCode::ClientRecordMalformed);
  record = Json::parse(payload);
  record["reads"][0]["key_bytes_base64"] = "";
  expectError(client_parser, record.dump(),
              MonitorErrorCode::ClientRecordMalformed);
  record = Json::parse(payload);
  record["reads"][0]["key_bytes_base64"] = "AAA";
  expectError(client_parser, record.dump(),
              MonitorErrorCode::ClientRecordMalformed);
  record = Json::parse(payload);
  record["reads"][0]["key_bytes_base64"] = "AB==";
  expectError(client_parser, record.dump(),
              MonitorErrorCode::ClientRecordMalformed);
  record = Json::parse(payload);
  record["reads"][0]["key_bytes_base64"] = "AAB=";
  expectError(client_parser, record.dump(),
              MonitorErrorCode::ClientRecordMalformed);
  record = Json::parse(payload);
  record["reads"][0]["key_bytes_base64"] = "AA=A";
  expectError(client_parser, record.dump(),
              MonitorErrorCode::ClientRecordMalformed);
  record = Json::parse(payload);
  record["reads"][0]["key_bytes_base64"] = "AA A";
  expectError(client_parser, record.dump(),
              MonitorErrorCode::ClientRecordMalformed);
  record = Json::parse(payload);
  record["reads"][0]["key_bytes_base64"] = "AA?=";
  expectError(client_parser, record.dump(),
              MonitorErrorCode::ClientRecordMalformed);
  record = Json::parse(payload);
  record["reads"][0].erase("value_bytes_base64");
  expectError(client_parser, record.dump(),
              MonitorErrorCode::ClientRecordMalformed);
  record = Json::parse(payload);
  record["reads"][1]["value_bytes_base64"] = "";
  expectError(client_parser, record.dump(),
              MonitorErrorCode::ClientRecordMalformed);
  record = Json::parse(payload);
  record["reads"][0]["source"] = "locking";
  expectError(client_parser, record.dump(),
              MonitorErrorCode::UnsupportedReadSource);
  record = Json::parse(payload);
  record["read_digest"] = std::string(64U, '0');
  expectError(client_parser, record.dump(),
              MonitorErrorCode::ClientReadDigestMismatch);
  record = Json::parse(payload);
  record["read_digest"] = std::string(64U, 'A');
  expectError(client_parser, record.dump(),
              MonitorErrorCode::ClientRecordMalformed);
}

void testCanonicalBinaryValidation() {
  const auto valid_key =
      CanonicalYcsbCodec::encodeKey("benchbase", "usertable", 42);
  const auto invalid_key_record = recordWithReads({ClientRead{
      .event_seq = 0,
      .key = CanonicalBytes{std::byte{0}},
      .state = ClientValueState::Absent,
      .value = std::nullopt,
      .source = ClientReadSource::Snapshot,
  }});
  BLACKBOX_CHECK(
      invalid_key_record["read_digest"].get<std::string>() ==
      "651fb612c9ed9648143cb5838fc3f19e03406ac32ae40b4e6d321cb41ab6295e");
  expectError(parser(), invalid_key_record.dump(),
              MonitorErrorCode::CanonicalEncodingFailed);

  const auto empty_value_record = recordWithReads({ClientRead{
      .event_seq = 0,
      .key = valid_key,
      .state = ClientValueState::Present,
      .value = CanonicalBytes{},
      .source = ClientReadSource::Snapshot,
  }});
  expectError(parser(), empty_value_record.dump(),
              MonitorErrorCode::CanonicalEncodingFailed);

  auto bad_tag = CanonicalBytes(10U, std::byte{0});
  bad_tag[0] = std::byte{2};
  const auto bad_tag_record = recordWithReads({ClientRead{
      .event_seq = 0,
      .key = valid_key,
      .state = ClientValueState::Present,
      .value = bad_tag,
      .source = ClientReadSource::Snapshot,
  }});
  expectError(parser(), bad_tag_record.dump(),
              MonitorErrorCode::CanonicalEncodingFailed);

  auto bad_utf8 = CanonicalBytes{
      std::byte{1},    std::byte{0}, std::byte{0}, std::byte{0}, std::byte{1},
      std::byte{0x80}, std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0},
      std::byte{0},    std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0},
  };
  const auto bad_utf8_record = recordWithReads({ClientRead{
      .event_seq = 0,
      .key = valid_key,
      .state = ClientValueState::Present,
      .value = std::move(bad_utf8),
      .source = ClientReadSource::Snapshot,
  }});
  expectError(parser(), bad_utf8_record.dump(),
              MonitorErrorCode::CanonicalEncodingFailed);

  const auto other_key =
      CanonicalYcsbCodec::encodeKey("benchbase", "other_table", 42);
  const auto other_table_record = recordWithReads({ClientRead{
      .event_seq = 0,
      .key = other_key,
      .state = ClientValueState::Absent,
      .value = std::nullopt,
      .source = ClientReadSource::Snapshot,
  }});
  expectError(parser(), other_table_record.dump(),
              MonitorErrorCode::CanonicalEncodingFailed);
}

void testCanonicalPaddingPaths() {
  std::array<std::optional<std::string>, 10> all_null{};
  std::array<std::optional<std::string>, 10> one_empty{};
  one_empty[0] = "";
  const std::vector<ClientRead> reads = {
      ClientRead{
          .event_seq = 0,
          .key = CanonicalYcsbCodec::encodeKey("benchbase", "tableaa", 1),
          .state = ClientValueState::Present,
          .value = valueWithFields(all_null),
          .source = ClientReadSource::Snapshot,
      },
      ClientRead{
          .event_seq = 1,
          .key = CanonicalYcsbCodec::encodeKey("benchbase", "tableone", 2),
          .state = ClientValueState::Present,
          .value = valueWithFields(one_empty),
          .source = ClientReadSource::Snapshot,
      },
  };
  const auto record = recordWithReads(reads);
  BLACKBOX_CHECK(
      record["reads"][0]["key_bytes_base64"].get<std::string>().ends_with(
          "=="));
  BLACKBOX_CHECK(
      record["reads"][1]["key_bytes_base64"].get<std::string>().ends_with('='));
  BLACKBOX_CHECK(
      record["reads"][0]["value_bytes_base64"].get<std::string>().ends_with(
          "=="));
  BLACKBOX_CHECK(
      record["reads"][1]["value_bytes_base64"].get<std::string>().ends_with(
          '='));
  const ClientTxnParser padded_parser(
      std::string(run_id), {std::string(session_id)},
      {"benchbase.tableaa", "benchbase.tableone"});
  const auto parsed = padded_parser.parse(record.dump());
  BLACKBOX_CHECK(parsed.reads == reads);
}

void testCounterValidation() {
  const auto payload =
      readFile(std::filesystem::path(BLACKBOX_PROTOCOL_EXAMPLE_DIR) /
               "client-txn-second.json");
  const auto client_parser = parser();
  auto record = Json::parse(payload);
  record["session_seq"] = 1.0;
  BLACKBOX_CHECK(client_parser.parse(record.dump()).session_seq == 1U);
  auto compact = Json::parse(payload).dump();
  auto position = compact.find("\"session_seq\":3");
  BLACKBOX_REQUIRE(position != std::string::npos);
  compact.replace(position, std::string_view("\"session_seq\":3").size(),
                  "\"session_seq\":1e0");
  BLACKBOX_CHECK(client_parser.parse(compact).session_seq == 1U);

  compact = Json::parse(payload).dump();
  position = compact.find("\"session_seq\":3");
  BLACKBOX_REQUIRE(position != std::string::npos);
  compact.replace(position, std::string_view("\"session_seq\":3").size(),
                  "\"session_seq\":1.00000000000000001");
  expectError(client_parser, compact, MonitorErrorCode::ClientRecordMalformed);

  compact = Json::parse(payload).dump();
  position = compact.find("\"read_count\":0");
  BLACKBOX_REQUIRE(position != std::string::npos);
  compact.replace(position, std::string_view("\"read_count\":0").size(),
                  "\"read_count\":1e-4000");
  expectError(client_parser, compact, MonitorErrorCode::ClientRecordMalformed);

  record["session_seq"] = 1.5;
  expectError(client_parser, record.dump(),
              MonitorErrorCode::ClientRecordMalformed);
  record = Json::parse(payload);
  record["session_seq"] = -1;
  expectError(client_parser, record.dump(),
              MonitorErrorCode::ClientRecordMalformed);
  record = Json::parse(payload);
  record["session_seq"] = "1";
  expectError(client_parser, record.dump(),
              MonitorErrorCode::ClientRecordMalformed);
  record = Json::parse(payload);
  record["session_seq"] = 9007199254740991ULL;
  record["correlation_id"]["txn_id"] = 9007199254740991ULL;
  BLACKBOX_CHECK(client_parser.parse(record.dump()).session_seq ==
                 9007199254740991ULL);
  record["session_seq"] = 9007199254740992ULL;
  expectError(client_parser, record.dump(),
              MonitorErrorCode::ClientRecordMalformed);
  record = Json::parse(payload);
  record["correlation_id"]["txn_id"] = 0;
  expectError(client_parser, record.dump(),
              MonitorErrorCode::ClientRecordMalformed);
  record = Json::parse(payload);
  record["correlation_id"]["txn_id"] = 2;
  expectError(client_parser, record.dump(),
              MonitorErrorCode::SessionAttemptProgressInvalid);
  record = Json::parse(payload);
  record["session_seq"] = 1;
  record["correlation_id"]["txn_id"] = 2;
  BLACKBOX_CHECK(client_parser.parse(record.dump()).correlation_id.txn_id ==
                 2U);
}

void testLimitsAndConfiguration() {
  const auto payload = readFile(
      std::filesystem::path(BLACKBOX_PROTOCOL_EXAMPLE_DIR) / "client-txn.json");
  const auto accepted = parser().parse(payload);
  std::size_t decoded_bytes = 0;
  std::size_t largest_key = 0;
  std::size_t largest_value = 0;
  for (const auto &read : accepted.reads) {
    decoded_bytes += read.key.size();
    largest_key = std::max(largest_key, read.key.size());
    if (read.value.has_value()) {
      decoded_bytes += read.value->size();
      largest_value = std::max(largest_value, read.value->size());
    }
  }
  const auto with_limits = [](const ClientTxnParserLimits limits) {
    return ClientTxnParser(std::string(run_id), {std::string(session_id)},
                           {"benchbase.usertable"}, limits);
  };

  auto limits = ClientTxnParserLimits{};
  limits.max_payload_bytes = payload.size() - 1U;
  expectError(with_limits(limits), payload,
              MonitorErrorCode::ResourceExhausted);
  limits.max_payload_bytes = payload.size();
  BLACKBOX_CHECK(with_limits(limits).parse(payload) == accepted);

  limits = ClientTxnParserLimits{};
  limits.max_key_bytes = largest_key - 1U;
  expectError(with_limits(limits), payload,
              MonitorErrorCode::ResourceExhausted);
  limits.max_key_bytes = largest_key;
  BLACKBOX_CHECK(with_limits(limits).parse(payload) == accepted);

  limits = ClientTxnParserLimits{};
  limits.max_value_bytes = largest_value - 1U;
  expectError(with_limits(limits), payload,
              MonitorErrorCode::ResourceExhausted);
  limits.max_value_bytes = largest_value;
  BLACKBOX_CHECK(with_limits(limits).parse(payload) == accepted);

  limits = ClientTxnParserLimits{};
  limits.max_decoded_bytes = decoded_bytes - 1U;
  expectError(with_limits(limits), payload,
              MonitorErrorCode::ResourceExhausted);
  limits.max_decoded_bytes = decoded_bytes;
  BLACKBOX_CHECK(with_limits(limits).parse(payload) == accepted);

  limits = ClientTxnParserLimits{};
  limits.max_reads = accepted.reads.size() - 1U;
  expectError(with_limits(limits), payload,
              MonitorErrorCode::ResourceExhausted);
  limits.max_reads = accepted.reads.size();
  BLACKBOX_CHECK(with_limits(limits).parse(payload) == accepted);

  expectError(parser(), "[[[[[[[[[[0]]]]]]]]]]",
              MonitorErrorCode::ResourceExhausted);

  bool rejected = false;
  try {
    (void)ClientTxnParser(std::string(run_id),
                          {std::string(session_id), std::string(session_id)},
                          {"benchbase.usertable"});
  } catch (const std::invalid_argument &) {
    rejected = true;
  }
  BLACKBOX_CHECK(rejected);

  rejected = false;
  try {
    (void)ClientTxnParser(std::string(run_id), {std::string(session_id)},
                          {"BenchBase.usertable"});
  } catch (const std::invalid_argument &) {
    rejected = true;
  }
  BLACKBOX_CHECK(rejected);

  rejected = false;
  try {
    limits = ClientTxnParserLimits{};
    limits.max_reads = 1001U;
    (void)with_limits(limits);
  } catch (const std::invalid_argument &) {
    rejected = true;
  }
  BLACKBOX_CHECK(rejected);
  BLACKBOX_CHECK(
      blackbox::toString(MonitorErrorCode::CanonicalEncodingFailed) ==
      "CANONICAL_ENCODING_FAILED");
}

} // namespace

int runClientTxnTests() {
  testGoldenRecords();
  testMalformedStructure();
  testRunSessionAndCorrelation();
  testReadValidation();
  testCanonicalBinaryValidation();
  testCanonicalPaddingPaths();
  testCounterValidation();
  testLimitsAndConfiguration();
  return 0;
}
