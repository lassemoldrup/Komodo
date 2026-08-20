#include "blackbox/protocol_records.h"

#include "test_support.h"

#include <nlohmann/json.hpp>

#include <array>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <sstream>
#include <string>
#include <string_view>

namespace {

using blackbox::MonitorError;
using blackbox::MonitorErrorCode;
using blackbox::ProtocolRecordKind;
using blackbox::ProtocolRecordParser;
using blackbox::ProtocolRecordParserLimits;
using blackbox::SessionMarkerEntry;
using Json = nlohmann::json;

constexpr std::string_view run_id = "00112233-4455-6677-8899-aabbccddeeff";

[[nodiscard]] std::string exampleText(const std::string_view name) {
  const auto path = std::filesystem::path(BLACKBOX_PROTOCOL_EXAMPLE_DIR) / name;
  std::ifstream input(path, std::ios::binary);
  BLACKBOX_CHECK(input.good());
  std::ostringstream result;
  result << input.rdbuf();
  return result.str();
}

[[nodiscard]] Json exampleJson(const std::string_view name) {
  return Json::parse(exampleText(name));
}

void expectError(const std::function<void()> &action,
                 const MonitorErrorCode expected) {
  try {
    action();
    BLACKBOX_CHECK(false);
  } catch (const MonitorError &error) {
    BLACKBOX_CHECK(error.code() == expected);
  }
}

void testKindsAndExamples() {
  ProtocolRecordParser parser;

  const auto manifest_payload = exampleText("run-manifest.json");
  BLACKBOX_CHECK(parser.kind(manifest_payload) ==
                 ProtocolRecordKind::RunManifest);
  const auto manifest = parser.parseRunManifest(manifest_payload);
  BLACKBOX_CHECK(manifest.run_id == run_id);
  BLACKBOX_CHECK(manifest.backend == "mariadb");
  BLACKBOX_CHECK(manifest.backend_source_id == "mariadb-main");
  BLACKBOX_CHECK(manifest.declared_isolation == "REPEATABLE_READ");
  BLACKBOX_CHECK(manifest.benchmark == "ycsb");
  BLACKBOX_CHECK(manifest.expected_session_ids.size() == 2U);
  BLACKBOX_CHECK(manifest.expected_session_ids[0] == "ycsb/worker/1");
  BLACKBOX_CHECK(manifest.application_table_allowlist ==
                 std::vector<std::string>{"benchbase.usertable"});
  BLACKBOX_CHECK(manifest.transaction_marker_table == "benchbase.session_txn");
  BLACKBOX_CHECK(manifest.control_table_allowlist ==
                 std::vector<std::string>{"benchbase.checker_run_fence"});
  BLACKBOX_CHECK(manifest.result_scope == "point_transaction_soww");

  const auto bootstrap_payload = exampleText("bootstrap-manifest.json");
  BLACKBOX_CHECK(parser.kind(bootstrap_payload) ==
                 ProtocolRecordKind::BootstrapManifest);
  const auto bootstrap = parser.parseBootstrapManifest(bootstrap_payload);
  BLACKBOX_CHECK(bootstrap.run_id == run_id);
  BLACKBOX_CHECK(bootstrap.table_row_counts.size() == 1U);
  BLACKBOX_CHECK(bootstrap.table_row_counts[0].table == "benchbase.usertable");
  BLACKBOX_CHECK(bootstrap.table_row_counts[0].row_count == 2U);
  BLACKBOX_CHECK(
      bootstrap.canonical_state_digest ==
      "71749720e062cf361dbad1e139c88d7d39b10debeddf18562d35cd80ae53afa8");

  const auto done_payload = exampleText("session-done.json");
  BLACKBOX_CHECK(parser.kind(done_payload) == ProtocolRecordKind::SessionDone);
  const auto done = parser.parseSessionDone(done_payload);
  BLACKBOX_CHECK(done.run_id == run_id);
  BLACKBOX_CHECK(done.session_id == "ycsb/worker/1");
  BLACKBOX_CHECK(done.final_session_seq == 3U);
  BLACKBOX_CHECK(done.logical_transactions_started == 3U);
  BLACKBOX_CHECK(done.successful_transactions == 2U);
  BLACKBOX_CHECK(done.terminal_aborted_transactions == 1U);
  BLACKBOX_CHECK(done.physical_attempts_started == 3U);
  BLACKBOX_CHECK(done.definitively_aborted_attempts == 1U);
  BLACKBOX_CHECK(done.published_transactions == 2U);
  BLACKBOX_CHECK(done.retry_attempts == 0U);
  BLACKBOX_CHECK(done.ambiguous_attempts == 0U);

  const auto empty_done =
      parser.parseSessionDone(exampleText("session-done-empty.json"));
  BLACKBOX_CHECK(empty_done.session_id == "ycsb/worker/2");
  BLACKBOX_CHECK(empty_done.final_session_seq == 0U);
  BLACKBOX_CHECK(
      empty_done.rolling_correlation_digest ==
      "7bff30ecd50b94ad4bbe9d5984d9e526bbb261716561d2ea9a72af52c4b506a8");

  const auto failed_payload = exampleText("run-failed.json");
  BLACKBOX_CHECK(parser.kind(failed_payload) == ProtocolRecordKind::RunFailed);
  const auto failed = parser.parseRunFailed(failed_payload);
  BLACKBOX_CHECK(failed.run_id == run_id);
  BLACKBOX_REQUIRE(failed.session_id.has_value());
  BLACKBOX_CHECK(*failed.session_id == "ycsb/worker/1");
  BLACKBOX_CHECK(failed.phase == "COMMIT");
  BLACKBOX_CHECK(failed.reason_code == "COMMIT_OUTCOME_AMBIGUOUS");

  BLACKBOX_CHECK(parser.kind(exampleText("client-txn.json")) ==
                 ProtocolRecordKind::ClientTxn);
  BLACKBOX_CHECK(blackbox::toString(ProtocolRecordKind::ClientTxn) ==
                 "client_txn");
}

void testManifestValidationAndBounds() {
  ProtocolRecordParser parser;
  auto manifest = exampleJson("run-manifest.json");

  {
    auto tpcc = manifest;
    tpcc["benchmark"] = "tpcc";
    BLACKBOX_CHECK(parser.parseRunManifest(tpcc.dump()).benchmark == "tpcc");
  }
  {
    auto future = manifest;
    future["benchmark"] = "FutureAdapter_1";
    BLACKBOX_CHECK(parser.parseRunManifest(future.dump()).benchmark ==
                   "FutureAdapter_1");
  }
  for (const auto invalid_name : {"", "1tpcc", "tpcc-adapter"}) {
    auto invalid = manifest;
    invalid["benchmark"] = invalid_name;
    expectError([&] { (void)parser.parseRunManifest(invalid.dump()); },
                MonitorErrorCode::ClientRecordMalformed);
  }
  {
    auto invalid = manifest;
    invalid["benchmark"] = std::string(65U, 'a');
    expectError([&] { (void)parser.parseRunManifest(invalid.dump()); },
                MonitorErrorCode::ClientRecordMalformed);
  }

  {
    auto invalid = manifest;
    invalid["backend"] = "postgres";
    expectError([&] { (void)parser.parseRunManifest(invalid.dump()); },
                MonitorErrorCode::ClientRecordMalformed);
  }
  {
    auto invalid = manifest;
    invalid["expected_session_ids"].push_back("ycsb/worker/1");
    expectError([&] { (void)parser.parseRunManifest(invalid.dump()); },
                MonitorErrorCode::ClientRecordMalformed);
  }
  {
    auto invalid = manifest;
    invalid["control_table_allowlist"] = Json::array({"benchbase.usertable"});
    expectError([&] { (void)parser.parseRunManifest(invalid.dump()); },
                MonitorErrorCode::ClientRecordMalformed);
  }
  {
    auto invalid = manifest;
    invalid["transaction_marker_table"] = "BenchBase.session_txn";
    expectError([&] { (void)parser.parseRunManifest(invalid.dump()); },
                MonitorErrorCode::ClientRecordMalformed);
  }
  {
    auto invalid = manifest;
    invalid["extra"] = true;
    expectError([&] { (void)parser.parseRunManifest(invalid.dump()); },
                MonitorErrorCode::ClientRecordMalformed);
  }
  {
    auto invalid = manifest;
    invalid.erase("run_id");
    expectError([&] { (void)parser.parseRunManifest(invalid.dump()); },
                MonitorErrorCode::ClientRecordMalformed);
  }
  {
    ProtocolRecordParser bounded(ProtocolRecordParserLimits{
        .max_payload_bytes = 4U * 1024U * 1024U,
        .max_expected_sessions = 1U,
        .max_table_entries = 1U,
    });
    expectError([&] { (void)bounded.parseRunManifest(manifest.dump()); },
                MonitorErrorCode::ResourceExhausted);
  }
  {
    ProtocolRecordParser bounded(ProtocolRecordParserLimits{
        .max_payload_bytes = 16U,
        .max_expected_sessions = 1U,
        .max_table_entries = 1U,
    });
    expectError([&] { (void)bounded.kind(manifest.dump()); },
                MonitorErrorCode::ResourceExhausted);
  }
}

void testBootstrapValidation() {
  ProtocolRecordParser parser;
  const auto bootstrap = exampleJson("bootstrap-manifest.json");
  {
    auto invalid = bootstrap;
    invalid["canonical_state_digest"] = std::string(64U, 'A');
    expectError([&] { (void)parser.parseBootstrapManifest(invalid.dump()); },
                MonitorErrorCode::ClientRecordMalformed);
  }
  {
    auto invalid = bootstrap;
    invalid["table_row_counts"] =
        Json::array({Json{{"table", "benchbase.ztable"}, {"row_count", 0}},
                     Json{{"table", "benchbase.atable"}, {"row_count", 0}}});
    expectError([&] { (void)parser.parseBootstrapManifest(invalid.dump()); },
                MonitorErrorCode::ClientRecordMalformed);
  }
  {
    auto invalid = bootstrap;
    invalid["table_row_counts"][0]["row_count"] = 9007199254740992ULL;
    expectError([&] { (void)parser.parseBootstrapManifest(invalid.dump()); },
                MonitorErrorCode::ClientRecordMalformed);
  }
  {
    auto invalid = bootstrap;
    invalid["table_row_counts"][0]["row_count"] = 1.5;
    expectError([&] { (void)parser.parseBootstrapManifest(invalid.dump()); },
                MonitorErrorCode::ClientRecordMalformed);
  }
}

void testSessionDoneValidation() {
  ProtocolRecordParser parser;
  const auto done = exampleJson("session-done.json");
  {
    auto invalid = done;
    invalid["final_session_seq"] = 4;
    expectError([&] { (void)parser.parseSessionDone(invalid.dump()); },
                MonitorErrorCode::SessionDoneMismatch);
  }
  {
    auto invalid = done;
    invalid["ambiguous_attempts"] = 1;
    invalid["physical_attempts_started"] = 4;
    invalid["retry_attempts"] = 1;
    expectError([&] { (void)parser.parseSessionDone(invalid.dump()); },
                MonitorErrorCode::SessionDoneMismatch);
  }
  {
    auto invalid = done;
    invalid["rolling_correlation_digest"] = std::string(64U, 'F');
    expectError([&] { (void)parser.parseSessionDone(invalid.dump()); },
                MonitorErrorCode::ClientRecordMalformed);
  }
  {
    auto integral = done;
    integral["final_session_seq"] = 3.0;
    BLACKBOX_CHECK(parser.parseSessionDone(integral.dump()).final_session_seq ==
                   3U);
  }
  {
    std::string fractional = done.dump();
    const std::string needle = "\"final_session_seq\":3";
    const auto position = fractional.find(needle);
    BLACKBOX_REQUIRE(position != std::string::npos);
    fractional.replace(position, needle.size(),
                       "\"final_session_seq\":3.0000000000000001");
    expectError([&] { (void)parser.parseSessionDone(fractional); },
                MonitorErrorCode::ClientRecordMalformed);
  }
  {
    auto unsafe = done;
    unsafe["final_session_seq"] = 9007199254740992ULL;
    expectError([&] { (void)parser.parseSessionDone(unsafe.dump()); },
                MonitorErrorCode::ClientRecordMalformed);
  }
}

void testFailureAndDiscriminatorValidation() {
  ProtocolRecordParser parser;
  const auto failed = exampleJson("run-failed.json");
  {
    auto session_contract = failed;
    session_contract["reason_code"] = "SESSION_CONTRACT_MISMATCH";
    BLACKBOX_CHECK(parser.parseRunFailed(session_contract.dump()).reason_code ==
                   "SESSION_CONTRACT_MISMATCH");
  }
  {
    auto invalid = failed;
    invalid["reason_code"] = "PASS";
    expectError([&] { (void)parser.parseRunFailed(invalid.dump()); },
                MonitorErrorCode::ClientRecordMalformed);
  }
  {
    auto invalid = failed;
    invalid["phase"] = "UNKNOWN";
    expectError([&] { (void)parser.parseRunFailed(invalid.dump()); },
                MonitorErrorCode::ClientRecordMalformed);
  }
  {
    auto invalid = failed;
    invalid["diagnostic"] = "";
    expectError([&] { (void)parser.parseRunFailed(invalid.dump()); },
                MonitorErrorCode::ClientRecordMalformed);
  }
  {
    auto coordinator = failed;
    coordinator.erase("session_id");
    coordinator["phase"] = "COORDINATOR";
    const auto parsed = parser.parseRunFailed(coordinator.dump());
    BLACKBOX_CHECK(!parsed.session_id.has_value());
  }
  {
    expectError([&] { (void)parser.kind("{\"kind\":\"unknown\"}"); },
                MonitorErrorCode::ClientRecordMalformed);
  }
  {
    const auto manifest = exampleText("run-manifest.json");
    const auto duplicate =
        std::string("{\"kind\":\"run_manifest\",") + manifest.substr(1U);
    expectError([&] { (void)parser.kind(duplicate); },
                MonitorErrorCode::ClientRecordMalformed);
  }
  {
    expectError([&] { (void)parser.kind("[]"); },
                MonitorErrorCode::ClientRecordMalformed);
  }
}

void testSessionMarkerDigests() {
  const auto seed = blackbox::sessionMarkerDigestSeed(run_id, "ycsb/worker/1");
  BLACKBOX_CHECK(
      blackbox::sessionMarkerDigestHex(seed) ==
      "20c58f889d34c7f08ec308d2737b9bccb498868e19773cec3b2b696f43a12170");
  const auto first = blackbox::sessionMarkerDigestStep(
      seed, SessionMarkerEntry{.session_seq = 1U, .txn_id = 1U});
  BLACKBOX_CHECK(
      blackbox::sessionMarkerDigestHex(first) ==
      "c0e8ea544d438285925a25fcdf53dee3e673b00f8887526e09871d5fb69a4029");
  const auto second = blackbox::sessionMarkerDigestStep(
      first, SessionMarkerEntry{.session_seq = 3U, .txn_id = 3U});
  BLACKBOX_CHECK(
      blackbox::sessionMarkerDigestHex(second) ==
      "69d3511b963663dd9215557af81380e3b6c779cf97505da7204f2e510a5d5dea");

  constexpr std::array entries = {
      SessionMarkerEntry{.session_seq = 1U, .txn_id = 1U},
      SessionMarkerEntry{.session_seq = 3U, .txn_id = 3U},
  };
  BLACKBOX_CHECK(blackbox::computeRollingSessionMarkerDigest(
                     run_id, "ycsb/worker/1", entries) ==
                 blackbox::sessionMarkerDigestHex(second));
  BLACKBOX_CHECK(
      blackbox::computeRollingSessionMarkerDigest(
          run_id, "ycsb/worker/2", std::span<const SessionMarkerEntry>{}) ==
      "7bff30ecd50b94ad4bbe9d5984d9e526bbb261716561d2ea9a72af52c4b506a8");

  expectError(
      [&] {
        (void)blackbox::sessionMarkerDigestStep(
            seed, SessionMarkerEntry{.session_seq = 0U, .txn_id = 1U});
      },
      MonitorErrorCode::ClientRecordMalformed);
}

} // namespace

int runProtocolRecordTests() {
  testKindsAndExamples();
  testManifestValidationAndBounds();
  testBootstrapValidation();
  testSessionDoneValidation();
  testFailureAndDiscriminatorValidation();
  testSessionMarkerDigests();
  return 0;
}
