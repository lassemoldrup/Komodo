#include "blackbox/client_txn.h"

#include "blackbox/canonical_tpcc_codec.h"
#include "blackbox/canonical_ycsb_codec.h"

#include "test_support.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <set>
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

[[nodiscard]] Json loadTpccClientFixture() {
  return Json::parse(
      readFile(std::filesystem::path(BLACKBOX_PROTOCOL_FIXTURE_DIR) /
               "mariadb_tpcc_client_txns.json"));
}

[[nodiscard]] ClientTxnParser tpccParser(const Json &fixture) {
  std::vector<std::string> sessions;
  for (const auto &session :
       fixture.at("run_manifest").at("expected_session_ids")) {
    sessions.push_back(session.get<std::string>());
  }
  return ClientTxnParser(
      fixture.at("run_manifest").at("run_id").get<std::string>(),
      std::move(sessions), blackbox::BenchmarkAdapterRegistry::require("tpcc"));
}

[[nodiscard]] ClientTxnParser parser() {
  return ClientTxnParser(std::string(run_id),
                         {std::string(session_id), "ycsb/worker/2"},
                         blackbox::BenchmarkAdapterRegistry::require("ycsb"));
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
          .key = CanonicalYcsbCodec::encodeKey("benchbase", "usertable", 1),
          .state = ClientValueState::Present,
          .value = valueWithFields(all_null),
          .source = ClientReadSource::Snapshot,
      },
      ClientRead{
          .event_seq = 1,
          .key = CanonicalYcsbCodec::encodeKey("benchbase", "usertable", 2),
          .state = ClientValueState::Present,
          .value = valueWithFields(one_empty),
          .source = ClientReadSource::Snapshot,
      },
  };
  const auto record = recordWithReads(reads);
  BLACKBOX_CHECK(
      record["reads"][0]["value_bytes_base64"].get<std::string>().ends_with(
          "=="));
  BLACKBOX_CHECK(
      record["reads"][1]["value_bytes_base64"].get<std::string>().ends_with(
          '='));
  const ClientTxnParser padded_parser(
      std::string(run_id), {std::string(session_id)},
      blackbox::BenchmarkAdapterRegistry::require("ycsb"));
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
                           blackbox::BenchmarkAdapterRegistry::require("ycsb"),
                           limits);
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
                          blackbox::BenchmarkAdapterRegistry::require("ycsb"));
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

void testAdapterOwnedTransactionMembership() {
  const ClientTxnParser tpcc_parser(
      std::string(run_id), {std::string(session_id)},
      blackbox::BenchmarkAdapterRegistry::require("tpcc"));
  const auto payload =
      readFile(std::filesystem::path(BLACKBOX_PROTOCOL_EXAMPLE_DIR) /
               "client-txn-second.json");
  constexpr std::array<std::string_view, 5> transactions = {
      "NewOrder", "Payment", "OrderStatus", "Delivery", "StockLevel"};
  for (const auto transaction : transactions) {
    auto record = Json::parse(payload);
    record["txn_type"] = transaction;
    BLACKBOX_CHECK(tpcc_parser.parse(record.dump()).txn_type == transaction);
  }

  auto wrong_adapter_transaction = Json::parse(payload);
  wrong_adapter_transaction["txn_type"] = "ycsb_update";
  expectError(tpcc_parser, wrong_adapter_transaction.dump(),
              MonitorErrorCode::UnsupportedTransaction);

  auto malformed_name = Json::parse(payload);
  malformed_name["txn_type"] = "New-Order";
  expectError(tpcc_parser, malformed_name.dump(),
              MonitorErrorCode::ClientRecordMalformed);

  auto read_record = recordWithReads({ClientRead{
      .event_seq = 0,
      .key = CanonicalYcsbCodec::encodeKey("benchbase", "warehouse", 1),
      .state = ClientValueState::Absent,
      .value = std::nullopt,
      .source = ClientReadSource::Snapshot,
  }});
  read_record["txn_type"] = "NewOrder";
  BLACKBOX_CHECK(tpcc_parser.parse(read_record.dump()).reads[0].state ==
                 ClientValueState::Absent);
}

void testTpccClientFixtureAndFailures() {
  const auto fixture = loadTpccClientFixture();
  const auto tpcc_parser = tpccParser(fixture);
  const auto &adapter = blackbox::BenchmarkAdapterRegistry::require("tpcc");
  std::set<std::string> transaction_types;
  std::size_t local_reads = 0U;
  std::size_t empty_present_values = 0U;
  std::size_t multirow_transactions = 0U;
  std::set<std::uint64_t> session_sequences;

  const auto check_transactions = [&](const Json &transactions,
                                      const bool semantic) {
    for (const auto &entry : transactions) {
      const auto &record = entry.at("client_txn");
      const auto parsed = tpcc_parser.parse(record.dump());
      BLACKBOX_CHECK(parsed.correlation_id.txn_id == parsed.session_seq);
      BLACKBOX_CHECK(session_sequences.insert(parsed.session_seq).second);
      BLACKBOX_CHECK(parsed.reads.size() == record.at("read_count"));
      transaction_types.insert(parsed.txn_type);
      if (parsed.reads.size() > 1U) {
        ++multirow_transactions;
      }
      if (semantic) {
        BLACKBOX_REQUIRE(entry.at("read_provenance").size() ==
                         parsed.reads.size());
      } else {
        BLACKBOX_CHECK(parsed.reads.empty());
      }
      for (std::size_t index = 0; index < parsed.reads.size(); ++index) {
        const auto &read = parsed.reads[index];
        const auto table =
            entry.at("read_provenance")[index].at("table").get<std::string>();
        const auto *codec = adapter.rowCodec(table);
        BLACKBOX_REQUIRE(codec != nullptr);
        codec->validateKey(read.key);
        if (read.value.has_value()) {
          codec->validatePresentValue(*read.value);
          if (read.value->empty()) {
            ++empty_present_values;
          }
        }
        if (read.source == ClientReadSource::Local) {
          ++local_reads;
        }
      }
    }
  };
  check_transactions(fixture.at("semantic_transactions"), true);
  check_transactions(fixture.at("synthetic_join_clients"), false);
  BLACKBOX_CHECK(session_sequences ==
                 std::set<std::uint64_t>({1U, 2U, 3U, 4U, 5U, 6U, 7U, 8U}));
  BLACKBOX_CHECK(transaction_types ==
                 std::set<std::string>({"NewOrder", "Payment", "OrderStatus",
                                        "Delivery", "StockLevel"}));
  BLACKBOX_CHECK(local_reads == 2U);
  BLACKBOX_CHECK(empty_present_values == 1U);
  BLACKBOX_CHECK(multirow_transactions >= 4U);
  BLACKBOX_CHECK(
      fixture.at("empty_result_assertions")[0].at("emitted_reads").empty());

  auto discarded = fixture.at("semantic_transactions")[1].at("client_txn");
  const auto &discarded_vector = fixture.at("discarded_attempt_reads")[0];
  discarded["reads"] = discarded_vector.at("reads");
  discarded["read_count"] = discarded_vector.at("reads").size();
  discarded["read_digest"] = discarded_vector.at("read_digest");
  const auto parsed_discarded = tpcc_parser.parse(discarded.dump());
  BLACKBOX_REQUIRE(parsed_discarded.reads.size() == 3U);
  BLACKBOX_CHECK(parsed_discarded.reads.back().state ==
                 ClientValueState::Absent);
  BLACKBOX_CHECK(!parsed_discarded.reads.back().value.has_value());
  BLACKBOX_CHECK(discarded_vector.at("discarded").get<bool>());
  BLACKBOX_CHECK(!discarded_vector.at("published").get<bool>());

  const auto history = blackbox::CanonicalTpccCodec::encodeRow(
      "benchbase", "history",
      Json{{"h_id", 9223372036854775807LL},
           {"h_c_id", 1},
           {"h_c_d_id", 1},
           {"h_c_w_id", 1},
           {"h_d_id", 1},
           {"h_w_id", 1},
           {"h_date", "2026-08-20T12:34:57Z"},
           {"h_amount", "10.00"},
           {"h_data", "history"}});
  const ClientRead history_read{.event_seq = 0U,
                                .key = history.key,
                                .state = ClientValueState::Present,
                                .value = history.value,
                                .source = ClientReadSource::Snapshot};
  auto history_record = fixture.at("semantic_transactions")[0].at("client_txn");
  history_record["reads"] = Json::array({readJson(history_read)});
  history_record["read_count"] = 1;
  history_record["read_digest"] =
      blackbox::computeClientReadDigest(std::span(&history_read, 1U));
  BLACKBOX_CHECK(tpcc_parser.parse(history_record.dump()).reads[0].key ==
                 history.key);

  auto unknown_transaction =
      fixture.at("semantic_transactions")[0].at("client_txn");
  unknown_transaction["txn_type"] = "UnknownTpcc";
  expectError(tpcc_parser, unknown_transaction.dump(),
              MonitorErrorCode::UnsupportedTransaction);

  auto unknown_table = fixture.at("semantic_transactions")[0].at("client_txn");
  unknown_table["reads"][0]["key_bytes_base64"] =
      encodeBase64(CanonicalYcsbCodec::encodeKey("benchbase", "usertable", 1));
  unknown_table["reads"][0]["value_state"] = "absent";
  unknown_table["reads"][0].erase("value_bytes_base64");
  expectError(tpcc_parser, unknown_table.dump(),
              MonitorErrorCode::CanonicalEncodingFailed);

  auto wrong_value = fixture.at("semantic_transactions")[0].at("client_txn");
  wrong_value["reads"][0]["value_bytes_base64"] = "AA==";
  expectError(tpcc_parser, wrong_value.dump(),
              MonitorErrorCode::CanonicalEncodingFailed);

  auto wrong_digest = fixture.at("semantic_transactions")[0].at("client_txn");
  wrong_digest["read_digest"] = std::string(64U, '0');
  expectError(tpcc_parser, wrong_digest.dump(),
              MonitorErrorCode::ClientReadDigestMismatch);
}

void testOrderStatusFixtureProjection() {
  const auto fixture = loadTpccClientFixture();
  const auto tpcc_parser = tpccParser(fixture);
  const auto &transactions = fixture.at("semantic_transactions");
  const auto found = std::find_if(
      transactions.begin(), transactions.end(), [](const Json &entry) {
        return entry.at("client_txn").at("txn_type") == "OrderStatus";
      });
  BLACKBOX_REQUIRE(found != transactions.end());
  const auto parsed = tpcc_parser.parse(found->at("client_txn").dump());
  BLACKBOX_CHECK(parsed.txn_type == "OrderStatus");
  BLACKBOX_CHECK(parsed.session_seq == 4U);
  BLACKBOX_CHECK(parsed.correlation_id.txn_id == 4U);
  BLACKBOX_CHECK(
      parsed.read_digest ==
      "a7445d293e9a689255d1c45d9fde52fc7d1d1a6d076d655ccf5e19499c09b19a");
  BLACKBOX_REQUIRE(parsed.reads.size() == 4U);
  for (std::size_t index = 0; index < parsed.reads.size(); ++index) {
    BLACKBOX_CHECK(parsed.reads[index].event_seq == index);
    BLACKBOX_CHECK(parsed.reads[index].state == ClientValueState::Present);
    BLACKBOX_CHECK(parsed.reads[index].source == ClientReadSource::Snapshot);
    BLACKBOX_REQUIRE(parsed.reads[index].value.has_value());
  }
  BLACKBOX_REQUIRE(parsed.reads[1].value->size() > 4U);
  BLACKBOX_CHECK(std::to_integer<unsigned int>(parsed.reads[1].value->at(4U)) ==
                 0U); // nullable o_carrier_id
  for (const auto index : {2U, 3U}) {
    BLACKBOX_REQUIRE(parsed.reads[index].value->size() > 4U);
    BLACKBOX_CHECK(std::to_integer<unsigned int>(parsed.reads[index].value->at(
                       4U)) == 0U); // nullable ol_delivery_d
  }
  BLACKBOX_CHECK(parsed.reads[2].key != parsed.reads[3].key);

  const auto &provenance = found->at("read_provenance");
  BLACKBOX_REQUIRE(provenance.size() == 4U);
  BLACKBOX_CHECK(provenance[0].at("table") == "benchbase.customer");
  BLACKBOX_CHECK(provenance[1].at("table") == "benchbase.oorder");
  BLACKBOX_CHECK(provenance[2].at("table") == "benchbase.order_line");
  BLACKBOX_CHECK(provenance[3].at("table") == "benchbase.order_line");
  BLACKBOX_CHECK(provenance[1].at("trace_statement_id") ==
                 "ordStatGetNewestOrdFullSQL");
  BLACKBOX_CHECK(provenance[2].at("trace_statement_id") ==
                 "ordStatGetOrderLinesFullSQL");
  BLACKBOX_CHECK(provenance[3].at("trace_statement_id") ==
                 "ordStatGetOrderLinesFullSQL");

  auto duplicate_sequence = found->at("client_txn");
  duplicate_sequence["reads"][3]["event_seq"] = 2;
  expectError(tpcc_parser, duplicate_sequence.dump(),
              MonitorErrorCode::ClientRecordMalformed);

  auto truncated_nullable = found->at("client_txn");
  truncated_nullable["reads"][3]["value_bytes_base64"] = "AA==";
  expectError(tpcc_parser, truncated_nullable.dump(),
              MonitorErrorCode::CanonicalEncodingFailed);

  auto locking_source = found->at("client_txn");
  locking_source["reads"][1]["source"] = "locking";
  expectError(tpcc_parser, locking_source.dump(),
              MonitorErrorCode::UnsupportedReadSource);

  const auto &done = fixture.at("session_done");
  BLACKBOX_CHECK(done.at("retry_attempts") == 0);
  BLACKBOX_CHECK(done.at("ambiguous_attempts") == 0);
  BLACKBOX_CHECK(done.at("physical_attempts_started") == 8);
  BLACKBOX_CHECK(done.at("published_transactions") == 8);
}

void testNewOrderFixtureProjectionAndDiscardedAttempt() {
  const auto fixture = loadTpccClientFixture();
  const auto tpcc_parser = tpccParser(fixture);
  const auto &transactions = fixture.at("semantic_transactions");
  const auto found = std::find_if(
      transactions.begin(), transactions.end(), [](const Json &entry) {
        return entry.at("client_txn").at("txn_type") == "NewOrder";
      });
  BLACKBOX_REQUIRE(found != transactions.end());

  const auto parsed = tpcc_parser.parse(found->at("client_txn").dump());
  BLACKBOX_CHECK(parsed.txn_type == "NewOrder");
  BLACKBOX_CHECK(parsed.session_seq == 2U);
  BLACKBOX_CHECK(parsed.correlation_id.txn_id == 2U);
  BLACKBOX_CHECK(
      parsed.read_digest ==
      "636722dfcda5e1f1880c50a1b45697567eb729f5724499c95a603a9ed905efa7");
  BLACKBOX_REQUIRE(parsed.reads.size() == 3U);
  for (std::size_t index = 0; index < parsed.reads.size(); ++index) {
    BLACKBOX_CHECK(parsed.reads[index].event_seq == index);
    BLACKBOX_CHECK(parsed.reads[index].state == ClientValueState::Present);
    BLACKBOX_CHECK(parsed.reads[index].source == ClientReadSource::Snapshot);
    BLACKBOX_REQUIRE(parsed.reads[index].value.has_value());
  }

  const auto &provenance = found->at("read_provenance");
  BLACKBOX_REQUIRE(provenance.size() == parsed.reads.size());
  const std::array<std::string_view, 3> expected_tables = {
      "benchbase.customer", "benchbase.warehouse", "benchbase.item"};
  const std::array<std::string_view, 3> expected_statements = {
      "stmtGetCustFullSQL", "stmtGetWhseFullSQL", "stmtGetItemFullSQL"};
  std::set<std::string> observed_tables;
  for (std::size_t index = 0; index < provenance.size(); ++index) {
    BLACKBOX_CHECK(provenance[index].at("event_seq") == index);
    BLACKBOX_CHECK(provenance[index].at("table") == expected_tables[index]);
    BLACKBOX_CHECK(provenance[index].at("trace_statement_id") ==
                   expected_statements[index]);
    observed_tables.insert(provenance[index].at("table").get<std::string>());
  }
  BLACKBOX_CHECK(
      observed_tables ==
      std::set<std::string>(
          {"benchbase.customer", "benchbase.warehouse", "benchbase.item"}));
  BLACKBOX_CHECK(!observed_tables.contains("benchbase.district"));
  BLACKBOX_CHECK(!observed_tables.contains("benchbase.stock"));

  const auto &discarded = fixture.at("discarded_attempt_reads");
  BLACKBOX_REQUIRE(discarded.size() == 1U);
  const auto &invalid_item = discarded[0];
  BLACKBOX_CHECK(invalid_item.at("name") == "new_order_invalid_item");
  BLACKBOX_CHECK(invalid_item.at("txn_type") == "NewOrder");
  BLACKBOX_CHECK(invalid_item.at("discarded").get<bool>());
  BLACKBOX_CHECK(!invalid_item.at("published").get<bool>());
  BLACKBOX_CHECK(
      invalid_item.at("read_digest") ==
      "7f48e9fbe9842e729ba774ada6dd23f6d7b5fd8a7d2686ab248abdc05a72d105");

  auto discarded_record = found->at("client_txn");
  discarded_record["reads"] = invalid_item.at("reads");
  discarded_record["read_count"] = invalid_item.at("reads").size();
  discarded_record["read_digest"] = invalid_item.at("read_digest");
  const auto parsed_discarded = tpcc_parser.parse(discarded_record.dump());
  BLACKBOX_REQUIRE(parsed_discarded.reads.size() == 3U);
  BLACKBOX_CHECK(parsed_discarded.reads[2].state == ClientValueState::Absent);
  BLACKBOX_CHECK(!parsed_discarded.reads[2].value.has_value());
  const auto *item_codec =
      blackbox::BenchmarkAdapterRegistry::require("tpcc").rowCodec(
          "benchbase.item");
  BLACKBOX_REQUIRE(item_codec != nullptr);
  item_codec->validateKey(parsed_discarded.reads[2].key);
  BLACKBOX_CHECK(parsed_discarded.reads[2].key ==
                 blackbox::CanonicalTpccCodec::encodeKey(
                     "benchbase", "item", Json{{"i_id", 999999}}));

  for (const auto field : {"semantic_transactions", "synthetic_join_clients"}) {
    for (const auto &entry : fixture.at(field)) {
      BLACKBOX_CHECK(entry.at("client_txn").at("read_digest") !=
                     invalid_item.at("read_digest"));
    }
  }

  auto missing_value = found->at("client_txn");
  missing_value["reads"][2].erase("value_bytes_base64");
  expectError(tpcc_parser, missing_value.dump(),
              MonitorErrorCode::ClientRecordMalformed);

  auto missing_event = found->at("client_txn");
  missing_event["reads"][1].erase("event_seq");
  expectError(tpcc_parser, missing_event.dump(),
              MonitorErrorCode::ClientRecordMalformed);

  auto wrong_item_value = found->at("client_txn");
  wrong_item_value["reads"][2]["value_bytes_base64"] = "AA==";
  expectError(tpcc_parser, wrong_item_value.dump(),
              MonitorErrorCode::CanonicalEncodingFailed);
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
  testAdapterOwnedTransactionMembership();
  testTpccClientFixtureAndFailures();
  testOrderStatusFixtureProjection();
  testNewOrderFixtureProjectionAndDiscardedAttempt();
  return 0;
}
