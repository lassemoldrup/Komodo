#include "blackbox/canonical_tpcc_codec.h"
#include "blackbox/canonical_ycsb_codec.h"
#include "blackbox/cdc_error.h"
#include "blackbox/cdc_types.h"
#include "blackbox/debezium_assembler.h"
#include "test_support.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <functional>
#include <initializer_list>
#include <limits>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using blackbox::CanonicalBytes;
using blackbox::CanonicalYcsbCodec;
using blackbox::CdcAssemblerError;
using blackbox::CdcErrorCode;
using blackbox::CdcStreamRecord;
using blackbox::CdcTransaction;
using blackbox::CdcTransactionKind;
using blackbox::DebeziumAssemblerConfig;
using blackbox::DebeziumTxnAssembler;
using blackbox::NormalizedValueState;
using blackbox::RunFencePhase;
using Json = nlohmann::json;

#ifndef BLACKBOX_PROTOCOL_FIXTURE_DIR
#error "BLACKBOX_PROTOCOL_FIXTURE_DIR must name the shared protocol fixtures"
#endif

constexpr std::string_view kFixtureRunId =
    "084cc5a1-3cbe-4fef-bb5c-43559a1c13a0";

[[nodiscard]] DebeziumAssemblerConfig assemblerConfig() {
  return DebeziumAssemblerConfig{
      .run_id = std::string(kFixtureRunId),
      .backend_source_id = "mariadb-main",
      .debezium_version = "3.6.1.Final",
      .backend_adapter = &blackbox::BackendAdapterRegistry::require("mariadb"),
      .benchmark_adapter = &blackbox::BenchmarkAdapterRegistry::require("ycsb"),
      .application_tables = {"benchbase.usertable"},
      .transaction_marker_table = "benchbase.session_txn",
      .run_fence_table = "benchbase.checker_run_fence",
  };
}

[[nodiscard]] Json loadFixtureNamed(const std::string_view name) {
  std::ifstream input(std::string(BLACKBOX_CDC_FIXTURE_DIR) + "/" +
                      std::string(name));
  if (!input) {
    throw std::runtime_error("failed to open captured MariaDB CDC fixture");
  }
  return Json::parse(input);
}

[[nodiscard]] Json loadTpccTransactionFixture() {
  std::ifstream input(std::string(BLACKBOX_PROTOCOL_FIXTURE_DIR) +
                      "/mariadb_tpcc_transactions.json");
  if (!input) {
    throw std::runtime_error("failed to open captured TPC-C CDC fixture");
  }
  return Json::parse(input);
}

[[nodiscard]] DebeziumAssemblerConfig tpccAssemblerConfig(const Json &fixture) {
  const auto &adapter = blackbox::BenchmarkAdapterRegistry::require("tpcc");
  std::vector<std::string> tables;
  tables.reserve(adapter.tables().size());
  for (const auto &table : adapter.tables()) {
    tables.emplace_back(table.name);
  }
  return DebeziumAssemblerConfig{
      .run_id = fixture.at("run_id").get<std::string>(),
      .backend_source_id = "mariadb-main",
      .debezium_version = fixture.at("debezium_version").get<std::string>(),
      .backend_adapter = &blackbox::BackendAdapterRegistry::require("mariadb"),
      .benchmark_adapter = &adapter,
      .application_tables = std::move(tables),
      .transaction_marker_table = "benchbase.session_txn",
      .run_fence_table = "benchbase.checker_run_fence",
  };
}

[[nodiscard]] Json tpccRawEntries(const Json &fixture) {
  auto result = fixture.at("startup_schema");
  for (const auto &transaction : fixture.at("transactions")) {
    for (const auto &entry : transaction.at("raw_entries")) {
      result.push_back(entry);
    }
  }
  return result;
}

[[nodiscard]] std::string bytesHex(const CanonicalBytes &bytes) {
  constexpr std::string_view digits = "0123456789abcdef";
  std::string result;
  result.reserve(bytes.size() * 2U);
  for (const auto byte : bytes) {
    const auto value = std::to_integer<unsigned int>(byte);
    result.push_back(digits[value >> 4U]);
    result.push_back(digits[value & 0x0fU]);
  }
  return result;
}

[[nodiscard]] Json loadFixture() {
  return loadFixtureNamed("mariadb_debezium_3_6_1_lifecycle.json");
}

[[nodiscard]] Json loadSnapshotSchemaFixture() {
  const auto full =
      loadFixtureNamed("mariadb_debezium_3_6_1_full_lifecycle.json");
  return Json(full.begin(), full.begin() + 10);
}

[[nodiscard]] Json prependStartupSchemaSnapshot(const Json &transactions) {
  auto combined = loadSnapshotSchemaFixture();
  for (const auto &entry : transactions) {
    combined.push_back(entry);
  }
  for (std::size_t index = 0; index < 10U; ++index) {
    combined[index][0] = std::to_string(index + 1U) + "-0";
  }
  return combined;
}

[[nodiscard]] std::vector<CdcStreamRecord>
recordsFromFixture(const Json &root) {
  if (!root.is_array()) {
    throw std::runtime_error("fixture root is not an array");
  }
  std::vector<CdcStreamRecord> records;
  records.reserve(root.size());
  for (const auto &entry : root) {
    if (!entry.is_array() || entry.size() != 2U || !entry[0].is_string() ||
        !entry[1].is_array() || entry[1].size() != 2U ||
        !entry[1][0].is_string() || !entry[1][1].is_string()) {
      throw std::runtime_error("fixture entry has an invalid XRANGE shape");
    }
    records.push_back(CdcStreamRecord{
        .stream_id = entry[0].get<std::string>(),
        .key_json = entry[1][0].get<std::string>(),
        .envelope_json = entry[1][1].get<std::string>(),
    });
  }
  return records;
}

[[nodiscard]] Json envelopeAt(const Json &root, const std::size_t index) {
  return Json::parse(root.at(index).at(1).at(1).get<std::string>());
}

void setEnvelope(Json &root, const std::size_t index, const Json &envelope) {
  root.at(index).at(1).at(1) = envelope.dump();
}

[[nodiscard]] Json keyAt(const Json &root, const std::size_t index) {
  return Json::parse(root.at(index).at(1).at(0).get<std::string>());
}

void setKey(Json &root, const std::size_t index, const Json &key) {
  root.at(index).at(1).at(0) = key.dump();
}

[[nodiscard]] Json &tpccTransaction(Json &fixture,
                                    const std::string_view name) {
  for (auto &transaction : fixture.at("transactions")) {
    if (transaction.at("name") == name) {
      return transaction;
    }
  }
  throw std::runtime_error("TPC-C fixture transaction is missing");
}

[[nodiscard]] Json &tpccRowEntry(Json &fixture,
                                 const std::string_view transaction_name,
                                 const std::string_view table,
                                 const std::size_t occurrence = 0U) {
  auto &entries = tpccTransaction(fixture, transaction_name).at("raw_entries");
  std::size_t seen = 0;
  for (auto &entry : entries) {
    const auto envelope = Json::parse(entry.at(1).at(1).get<std::string>());
    if (envelope.contains("op") && envelope.at("source").at("table") == table) {
      if (seen == occurrence) {
        return entry;
      }
      ++seen;
    }
  }
  throw std::runtime_error("TPC-C fixture row entry is missing");
}

[[nodiscard]] Json entryEnvelope(const Json &entry) {
  return Json::parse(entry.at(1).at(1).get<std::string>());
}

void setEntryEnvelope(Json &entry, const Json &envelope) {
  entry.at(1).at(1) = envelope.dump();
}

[[nodiscard]] Json entryKey(const Json &entry) {
  return Json::parse(entry.at(1).at(0).get<std::string>());
}

void setEntryKey(Json &entry, const Json &key) {
  entry.at(1).at(0) = key.dump();
}

void renumberStreamIds(Json &root) {
  for (std::size_t index = 0; index < root.size(); ++index) {
    root[index][0] = std::to_string(1000000U + index) + "-0";
  }
}

[[nodiscard]] Json
selectEntries(const Json &root,
              const std::initializer_list<std::size_t> indexes) {
  Json selected = Json::array();
  for (const auto index : indexes) {
    selected.push_back(root.at(index));
  }
  renumberStreamIds(selected);
  return selected;
}

void rewriteStructuralId(Json &entry, const std::string &identifier) {
  auto key = Json::parse(entry[1][0].get<std::string>());
  auto envelope = Json::parse(entry[1][1].get<std::string>());
  if (key.contains("id")) {
    key["id"] = identifier;
  }
  if (envelope.contains("id")) {
    envelope["id"] = identifier;
  }
  if (envelope.contains("transaction")) {
    envelope["transaction"]["id"] = identifier;
    envelope["source"]["gtid"] = identifier;
  }
  entry[1][0] = key.dump();
  entry[1][1] = envelope.dump();
}

void expectError(Json root, const CdcErrorCode expected,
                 DebeziumAssemblerConfig config = assemblerConfig(),
                 const bool prepend_startup = true) {
  if (prepend_startup) {
    root = prependStartupSchemaSnapshot(root);
  }
  DebeziumTxnAssembler assembler(std::move(config));
  bool observed = false;
  try {
    for (const auto &record : recordsFromFixture(root)) {
      (void)assembler.accept(record);
    }
    assembler.finish();
  } catch (const CdcAssemblerError &error) {
    observed = true;
    BLACKBOX_CHECK(error.code() == expected);
    BLACKBOX_CHECK(assembler.failed());
    BLACKBOX_CHECK(assembler.errorCode() == expected);
    try {
      assembler.finish();
      BLACKBOX_CHECK(false);
    } catch (const CdcAssemblerError &sticky) {
      BLACKBOX_CHECK(sticky.code() == expected);
    }
  } catch (...) {
    BLACKBOX_CHECK(false);
    observed = true;
  }
  BLACKBOX_CHECK(observed);
}

[[nodiscard]] std::vector<CdcTransaction>
replay(Json root, DebeziumAssemblerConfig config = assemblerConfig(),
       const bool prepend_startup = true) {
  if (prepend_startup) {
    root = prependStartupSchemaSnapshot(root);
  }
  DebeziumTxnAssembler assembler(std::move(config));
  std::vector<CdcTransaction> transactions;
  for (const auto &record : recordsFromFixture(root)) {
    auto completed = assembler.accept(record);
    if (completed.has_value()) {
      transactions.push_back(std::move(*completed));
    }
  }
  assembler.finish();
  BLACKBOX_CHECK(assembler.completedCount() == transactions.size());
  BLACKBOX_CHECK(assembler.activeTransactionCount() == 0U);
  BLACKBOX_CHECK(assembler.activeBytes() == 0U);
  BLACKBOX_CHECK(assembler.retainedIdentityBytes() > 0U);
  return transactions;
}

[[nodiscard]] std::array<std::optional<std::string>, 10>
fields(std::optional<std::string> field1 = std::nullopt,
       std::optional<std::string> field2 = std::nullopt,
       std::optional<std::string> field3 = std::nullopt,
       std::optional<std::string> field4 = std::nullopt,
       std::optional<std::string> field5 = std::nullopt,
       std::optional<std::string> field6 = std::nullopt,
       std::optional<std::string> field7 = std::nullopt,
       std::optional<std::string> field8 = std::nullopt,
       std::optional<std::string> field9 = std::nullopt,
       std::optional<std::string> field10 = std::nullopt) {
  return {std::move(field1), std::move(field2), std::move(field3),
          std::move(field4), std::move(field5), std::move(field6),
          std::move(field7), std::move(field8), std::move(field9),
          std::move(field10)};
}

void checkCapturedStartupSchemaSnapshot() {
  const auto captured = loadSnapshotSchemaFixture();
  BLACKBOX_REQUIRE(captured.size() == 10U);

  DebeziumTxnAssembler snapshot_only(assemblerConfig());
  for (const auto &record : recordsFromFixture(captured)) {
    BLACKBOX_CHECK(!snapshot_only.accept(record).has_value());
  }
  snapshot_only.finish();
  BLACKBOX_CHECK(snapshot_only.completedCount() == 0U);
  BLACKBOX_CHECK(snapshot_only.activeTransactionCount() == 0U);
  BLACKBOX_CHECK(snapshot_only.activeBytes() == 0U);
  BLACKBOX_CHECK(snapshot_only.retainedIdentityBytes() == 0U);

  auto followed_by_run = captured;
  const auto lifecycle = loadFixture();
  for (const auto &entry : lifecycle) {
    followed_by_run.push_back(entry);
  }
  renumberStreamIds(followed_by_run);
  BLACKBOX_CHECK(replay(followed_by_run, assemblerConfig(), false).size() ==
                 11U);

  auto non_snapshot = captured;
  auto envelope = envelopeAt(non_snapshot, 0U);
  envelope["source"]["snapshot"] = "false";
  setEnvelope(non_snapshot, 0U, envelope);
  expectError(std::move(non_snapshot), CdcErrorCode::DdlObserved,
              assemblerConfig(), false);

  auto wrong_source = captured;
  envelope = envelopeAt(wrong_source, 0U);
  envelope["source"]["name"] = "other-source";
  setEnvelope(wrong_source, 0U, envelope);
  expectError(std::move(wrong_source), CdcErrorCode::DdlObserved,
              assemblerConfig(), false);

  auto malformed = captured;
  envelope = envelopeAt(malformed, 0U);
  envelope["tableChanges"] = Json::object();
  setEnvelope(malformed, 0U, envelope);
  expectError(std::move(malformed), CdcErrorCode::DdlObserved,
              assemblerConfig(), false);

  auto wrong_application_type = captured;
  envelope = envelopeAt(wrong_application_type, 9U);
  envelope["tableChanges"][0]["table"]["columns"][0]["typeName"] = "BIGINT";
  envelope["tableChanges"][0]["table"]["columns"][0]["typeExpression"] =
      "BIGINT";
  setEnvelope(wrong_application_type, 9U, envelope);
  expectError(std::move(wrong_application_type), CdcErrorCode::DdlObserved,
              assemblerConfig(), false);

  auto wrong_application_nullability = captured;
  envelope = envelopeAt(wrong_application_nullability, 9U);
  envelope["tableChanges"][0]["table"]["columns"][1]["optional"] = false;
  setEnvelope(wrong_application_nullability, 9U, envelope);
  expectError(std::move(wrong_application_nullability),
              CdcErrorCode::DdlObserved, assemblerConfig(), false);

  auto wrong_application_key = captured;
  envelope = envelopeAt(wrong_application_key, 9U);
  envelope["tableChanges"][0]["table"]["primaryKeyColumnNames"] =
      Json::array({"field1"});
  setEnvelope(wrong_application_key, 9U, envelope);
  expectError(std::move(wrong_application_key), CdcErrorCode::DdlObserved,
              assemblerConfig(), false);

  auto wrong_application_order = captured;
  envelope = envelopeAt(wrong_application_order, 9U);
  std::swap(envelope["tableChanges"][0]["table"]["columns"][0],
            envelope["tableChanges"][0]["table"]["columns"][1]);
  setEnvelope(wrong_application_order, 9U, envelope);
  expectError(std::move(wrong_application_order), CdcErrorCode::DdlObserved,
              assemblerConfig(), false);

  auto wrong_application_collation = captured;
  envelope = envelopeAt(wrong_application_collation, 9U);
  auto ddl = envelope["ddl"].get<std::string>();
  const auto collation = ddl.find("utf8mb4_bin");
  BLACKBOX_REQUIRE(collation != std::string::npos);
  ddl.replace(collation, std::string_view("utf8mb4_bin").size(),
              "utf8mb4_general_ci");
  envelope["ddl"] = std::move(ddl);
  setEnvelope(wrong_application_collation, 9U, envelope);
  expectError(std::move(wrong_application_collation), CdcErrorCode::DdlObserved,
              assemblerConfig(), false);

  auto incomplete = captured;
  incomplete.erase(incomplete.end() - 1);
  incomplete.push_back(lifecycle.at(0U));
  renumberStreamIds(incomplete);
  expectError(std::move(incomplete), CdcErrorCode::DdlObserved,
              assemblerConfig(), false);

  auto missing_known_table = captured;
  missing_known_table.erase(missing_known_table.begin() + 8);
  renumberStreamIds(missing_known_table);
  expectError(std::move(missing_known_table), CdcErrorCode::DdlObserved,
              assemblerConfig(), false);

  auto after_begin = captured;
  after_begin.push_back(lifecycle.at(0U));
  after_begin.push_back(captured.at(0U));
  renumberStreamIds(after_begin);
  expectError(std::move(after_begin), CdcErrorCode::DdlObserved,
              assemblerConfig(), false);

  auto after_run_start = captured;
  for (const auto index : {0U, 1U, 2U}) {
    after_run_start.push_back(lifecycle.at(index));
  }
  after_run_start.push_back(captured.at(0U));
  renumberStreamIds(after_run_start);
  expectError(std::move(after_run_start), CdcErrorCode::DdlObserved,
              assemblerConfig(), false);

  auto after_last = captured;
  after_last.push_back(captured.at(0U));
  renumberStreamIds(after_last);
  expectError(std::move(after_last), CdcErrorCode::DdlObserved,
              assemblerConfig(), false);

  auto duplicate_drop = captured;
  duplicate_drop.insert(duplicate_drop.begin() + 2, duplicate_drop.at(1U));
  renumberStreamIds(duplicate_drop);
  expectError(std::move(duplicate_drop), CdcErrorCode::DdlObserved,
              assemblerConfig(), false);

  auto reordered_drops = captured;
  std::swap(reordered_drops[1][1], reordered_drops[2][1]);
  expectError(std::move(reordered_drops), CdcErrorCode::DdlObserved,
              assemblerConfig(), false);

  auto reordered_database_records = captured;
  std::swap(reordered_database_records[4][1], reordered_database_records[5][1]);
  expectError(std::move(reordered_database_records), CdcErrorCode::DdlObserved,
              assemblerConfig(), false);

  auto reordered_creates = captured;
  std::swap(reordered_creates[7][1], reordered_creates[8][1]);
  expectError(std::move(reordered_creates), CdcErrorCode::DdlObserved,
              assemblerConfig(), false);

  auto duplicate_create = captured;
  duplicate_create.insert(duplicate_create.begin() + 8,
                          duplicate_create.at(7U));
  renumberStreamIds(duplicate_create);
  expectError(std::move(duplicate_create), CdcErrorCode::DdlObserved,
              assemblerConfig(), false);

  expectError(lifecycle, CdcErrorCode::CdcRecordMalformed, assemblerConfig(),
              false);
}

void checkCapturedLifecycleAndNormalization() {
  const auto transactions = replay(loadFixture());
  BLACKBOX_REQUIRE(transactions.size() == 11U);
  for (std::size_t index = 0; index < transactions.size(); ++index) {
    BLACKBOX_CHECK(transactions[index].cdc_seq == index);
    BLACKBOX_CHECK(!transactions[index].begin_stream_id.empty());
    BLACKBOX_CHECK(!transactions[index].end_stream_id.empty());
  }

  BLACKBOX_CHECK(transactions[0].kind == CdcTransactionKind::RunControl);
  BLACKBOX_CHECK(transactions[0].fence_phase == RunFencePhase::RunStart);
  BLACKBOX_CHECK(transactions[0].writes.empty());
  BLACKBOX_CHECK(!transactions[0].correlation_id.has_value());

  BLACKBOX_CHECK(transactions[1].kind == CdcTransactionKind::BootstrapApp);
  BLACKBOX_REQUIRE(transactions[1].writes.size() == 2U);
  BLACKBOX_CHECK(transactions[1].writes[0].key ==
                 CanonicalYcsbCodec::encodeKey("benchbase", "usertable", 42));
  BLACKBOX_CHECK(transactions[1].writes[0].state ==
                 NormalizedValueState::Present);
  const auto mixed =
      fields(std::nullopt, std::string(""), std::string("A"), std::string("é"),
             std::string("中"), std::string("0"), std::string("hello"),
             std::string("🙂"), std::string("a\0b", 3), std::string("last"));
  BLACKBOX_CHECK(transactions[1].writes[0].value ==
                 CanonicalYcsbCodec::encodeValue(mixed));
  BLACKBOX_CHECK(transactions[1].writes[1].key ==
                 CanonicalYcsbCodec::encodeKey("benchbase", "usertable", -1));
  BLACKBOX_CHECK(transactions[1].writes[1].value ==
                 CanonicalYcsbCodec::encodeValue(fields()));

  BLACKBOX_CHECK(transactions[2].kind == CdcTransactionKind::RunControl);
  BLACKBOX_CHECK(transactions[2].fence_phase == RunFencePhase::LoadFinished);

  BLACKBOX_CHECK(transactions[3].kind == CdcTransactionKind::WorkloadMarked);
  BLACKBOX_REQUIRE(transactions[3].correlation_id.has_value());
  BLACKBOX_CHECK(transactions[3].correlation_id->run_id == kFixtureRunId);
  BLACKBOX_CHECK(transactions[3].correlation_id->session_id == "ycsb/worker/1");
  BLACKBOX_CHECK(transactions[3].correlation_id->txn_id == 1U);
  BLACKBOX_REQUIRE(transactions[3].writes.size() == 1U);
  BLACKBOX_CHECK(transactions[3].writes[0].value ==
                 CanonicalYcsbCodec::encodeValue(fields(
                     std::string("changed"), std::string(""), std::string("A"),
                     std::string("é"), std::string("中"), std::string("0"),
                     std::string("hello"), std::string("🙂"),
                     std::string("a\0b", 3), std::string("last"))));

  // A real marker-only transaction survives assembly with no application
  // writes, and its connector GTID is not part of the exposed correlation.
  BLACKBOX_CHECK(transactions[4].kind == CdcTransactionKind::WorkloadMarked);
  BLACKBOX_REQUIRE(transactions[4].correlation_id.has_value());
  BLACKBOX_CHECK(transactions[4].correlation_id->txn_id == 2U);
  BLACKBOX_CHECK(transactions[4].writes.empty());

  // Insert/update/update/delete is one touched Absent key, not an empty set.
  BLACKBOX_REQUIRE(transactions[5].correlation_id.has_value());
  BLACKBOX_CHECK(transactions[5].correlation_id->txn_id == 3U);
  BLACKBOX_REQUIRE(transactions[5].writes.size() == 1U);
  BLACKBOX_CHECK(transactions[5].writes[0].key ==
                 CanonicalYcsbCodec::encodeKey("benchbase", "usertable", 100));
  BLACKBOX_CHECK(transactions[5].writes[0].state ==
                 NormalizedValueState::Absent);
  BLACKBOX_CHECK(!transactions[5].writes[0].value.has_value());

  BLACKBOX_REQUIRE(transactions[6].correlation_id.has_value());
  BLACKBOX_CHECK(transactions[6].correlation_id->txn_id == 4U);
  BLACKBOX_REQUIRE(transactions[6].writes.size() == 1U);
  BLACKBOX_CHECK(transactions[6].writes[0].state ==
                 NormalizedValueState::Present);
  BLACKBOX_REQUIRE(transactions[7].correlation_id.has_value());
  BLACKBOX_CHECK(transactions[7].correlation_id->txn_id == 5U);
  BLACKBOX_REQUIRE(transactions[7].writes.size() == 1U);
  BLACKBOX_CHECK(transactions[7].writes[0].state ==
                 NormalizedValueState::Present);
  BLACKBOX_CHECK(transactions[7].writes[0].value ==
                 transactions[6].writes[0].value);
  BLACKBOX_REQUIRE(transactions[8].correlation_id.has_value());
  BLACKBOX_CHECK(transactions[8].correlation_id->txn_id == 6U);
  BLACKBOX_REQUIRE(transactions[8].writes.size() == 1U);
  BLACKBOX_CHECK(transactions[8].writes[0].state ==
                 NormalizedValueState::Absent);

  // MariaDB emits a primary-key UPDATE as delete+insert. Both touched keys
  // remain, deterministically sorted by canonical bytes.
  BLACKBOX_REQUIRE(transactions[9].correlation_id.has_value());
  BLACKBOX_CHECK(transactions[9].correlation_id->txn_id == 7U);
  BLACKBOX_REQUIRE(transactions[9].writes.size() == 2U);
  BLACKBOX_CHECK(transactions[9].writes[0].key ==
                 CanonicalYcsbCodec::encodeKey("benchbase", "usertable", -2));
  BLACKBOX_CHECK(transactions[9].writes[0].state ==
                 NormalizedValueState::Present);
  BLACKBOX_CHECK(transactions[9].writes[1].key ==
                 CanonicalYcsbCodec::encodeKey("benchbase", "usertable", -1));
  BLACKBOX_CHECK(transactions[9].writes[1].state ==
                 NormalizedValueState::Absent);

  BLACKBOX_CHECK(transactions[10].kind == CdcTransactionKind::RunControl);
  BLACKBOX_CHECK(transactions[10].fence_phase == RunFencePhase::EndFence);
}

void checkMeasurementMetrics() {
  const auto captured =
      loadFixtureNamed("mariadb_debezium_3_6_1_full_lifecycle.json");
  const auto records = recordsFromFixture(captured);
  DebeziumTxnAssembler assembler(assemblerConfig());
  std::uint64_t expected_raw_bytes = 0U;
  for (const auto &record : records) {
    expected_raw_bytes += static_cast<std::uint64_t>(record.key_json.size());
    expected_raw_bytes +=
        static_cast<std::uint64_t>(record.envelope_json.size());
    (void)assembler.accept(record);
  }
  assembler.finish();

  const auto metrics = assembler.metrics();
  BLACKBOX_CHECK(expected_raw_bytes == 30599U);
  BLACKBOX_CHECK(metrics.accepted_raw_records == 55U);
  BLACKBOX_CHECK(metrics.accepted_raw_bytes == expected_raw_bytes);
  BLACKBOX_CHECK(metrics.startup_schema_records == 10U);
  BLACKBOX_CHECK(metrics.begin_metadata_records == 11U);
  BLACKBOX_CHECK(metrics.end_metadata_records == 11U);
  BLACKBOX_CHECK(metrics.row_records == 23U);
  BLACKBOX_CHECK(metrics.completed_transactions == 11U);
  BLACKBOX_CHECK(metrics.completed_run_control_transactions == 3U);
  BLACKBOX_CHECK(metrics.completed_bootstrap_transactions == 1U);
  BLACKBOX_CHECK(metrics.completed_workload_transactions == 7U);
  BLACKBOX_CHECK(metrics.raw_row_events == 23U);
  BLACKBOX_CHECK(metrics.normalized_write_events == 9U);
  BLACKBOX_CHECK(metrics.active_assembly_bytes == 0U);
  BLACKBOX_CHECK(metrics.active_assembly_high_water_bytes > 0U);
  BLACKBOX_CHECK(metrics.retained_identity_bytes > 0U);
}

void checkStructuralFailuresAreClosed() {
  const auto captured = loadFixture();

  auto row_without_begin = captured;
  row_without_begin.erase(row_without_begin.begin());
  expectError(std::move(row_without_begin), CdcErrorCode::CdcBeginMissing);

  auto incomplete = selectEntries(captured, {0U, 1U});
  expectError(std::move(incomplete), CdcErrorCode::CdcEndMissing);

  auto duplicate = captured;
  duplicate.insert(duplicate.begin() + 1, duplicate.at(0));
  expectError(std::move(duplicate), CdcErrorCode::CdcDuplicateConflict);

  auto cursor_sentinel = captured;
  cursor_sentinel[0][0] = "0-0";
  expectError(std::move(cursor_sentinel), CdcErrorCode::CdcOrderInvalid);

  auto reordered = captured;
  std::swap(reordered[1][0], reordered[2][0]);
  expectError(std::move(reordered), CdcErrorCode::CdcOrderInvalid);

  auto ordinal_gap = captured;
  auto envelope = envelopeAt(ordinal_gap, 1U);
  envelope["transaction"]["total_order"] = 2;
  setEnvelope(ordinal_gap, 1U, envelope);
  expectError(std::move(ordinal_gap), CdcErrorCode::CdcEventOrdinalInvalid);

  auto collection_gap = captured;
  envelope = envelopeAt(collection_gap, 1U);
  envelope["transaction"]["data_collection_order"] = 2;
  setEnvelope(collection_gap, 1U, envelope);
  expectError(std::move(collection_gap), CdcErrorCode::CdcEventOrdinalInvalid);

  auto wrong_total = captured;
  envelope = envelopeAt(wrong_total, 2U);
  envelope["event_count"] = 2;
  setEnvelope(wrong_total, 2U, envelope);
  expectError(std::move(wrong_total), CdcErrorCode::CdcEventCountMismatch);

  auto wrong_table_count = captured;
  envelope = envelopeAt(wrong_table_count, 2U);
  envelope["data_collections"][0]["event_count"] = 2;
  setEnvelope(wrong_table_count, 2U, envelope);
  expectError(std::move(wrong_table_count),
              CdcErrorCode::CdcEventCountMismatch);

  auto mismatched_source_id = captured;
  envelope = envelopeAt(mismatched_source_id, 1U);
  envelope["source"]["gtid"] = "different-structural-id";
  setEnvelope(mismatched_source_id, 1U, envelope);
  expectError(std::move(mismatched_source_id),
              CdcErrorCode::CdcTransactionIdMismatch);

  auto mismatched_end_id = captured;
  envelope = envelopeAt(mismatched_end_id, 2U);
  envelope["id"] = "different-structural-id";
  setEnvelope(mismatched_end_id, 2U, envelope);
  expectError(std::move(mismatched_end_id),
              CdcErrorCode::CdcTransactionIdMismatch);

  auto duplicate_json_key = selectEntries(captured, {0U});
  duplicate_json_key[0][1][1] =
      R"({"status":"BEGIN","status":"BEGIN","id":"0-1-9","event_count":null,"data_collections":null,"ts_ms":1})";
  expectError(std::move(duplicate_json_key), CdcErrorCode::CdcRecordMalformed);

  auto unexpected_field = captured;
  envelope = envelopeAt(unexpected_field, 1U);
  envelope["unknown"] = true;
  setEnvelope(unexpected_field, 1U, envelope);
  expectError(std::move(unexpected_field), CdcErrorCode::CdcRecordMalformed);

  auto unknown_table = captured;
  envelope = envelopeAt(unknown_table, 1U);
  envelope["source"]["table"] = "other_table";
  setEnvelope(unknown_table, 1U, envelope);
  expectError(std::move(unknown_table), CdcErrorCode::UnexpectedTable);

  auto unsupported_operation = captured;
  envelope = envelopeAt(unsupported_operation, 1U);
  envelope["op"] = "r";
  setEnvelope(unsupported_operation, 1U, envelope);
  expectError(std::move(unsupported_operation),
              CdcErrorCode::CdcRecordMalformed);

  auto snapshot_row = captured;
  envelope = envelopeAt(snapshot_row, 1U);
  envelope["source"]["snapshot"] = "true";
  setEnvelope(snapshot_row, 1U, envelope);
  expectError(std::move(snapshot_row), CdcErrorCode::ExternalWriterObserved);

  auto event_limited = assemblerConfig();
  event_limited.max_events_per_transaction = 1U;
  expectError(captured, CdcErrorCode::ResourceExhausted,
              std::move(event_limited));

  auto byte_limited = assemblerConfig();
  byte_limited.max_active_bytes = 1U;
  expectError(captured, CdcErrorCode::ResourceExhausted,
              std::move(byte_limited));

  auto completion_limited = assemblerConfig();
  completion_limited.max_completed_transactions = 1U;
  expectError(captured, CdcErrorCode::ResourceExhausted,
              std::move(completion_limited));

  auto identity_limited = assemblerConfig();
  identity_limited.max_retained_identity_bytes = 1U;
  expectError(captured, CdcErrorCode::ResourceExhausted,
              std::move(identity_limited));

  DebeziumTxnAssembler missing_snapshot(assemblerConfig());
  try {
    missing_snapshot.finish();
    BLACKBOX_CHECK(false);
  } catch (const CdcAssemblerError &error) {
    BLACKBOX_CHECK(error.code() == CdcErrorCode::CdcRecordMalformed);
  }

  DebeziumTxnAssembler sealed(assemblerConfig());
  for (const auto &record : recordsFromFixture(loadSnapshotSchemaFixture())) {
    (void)sealed.accept(record);
  }
  sealed.finish();
  try {
    (void)sealed.accept(recordsFromFixture(captured).front());
    BLACKBOX_CHECK(false);
  } catch (const CdcAssemblerError &error) {
    BLACKBOX_CHECK(error.code() == CdcErrorCode::CdcOrderInvalid);
    BLACKBOX_CHECK(sealed.failed());
  }
}

void checkMarkerAndFenceFailuresAreClosed() {
  const auto captured = loadFixture();

  // Remove the first workload marker, then make END honestly describe the
  // remaining row. This is structurally complete but uncorrelatable evidence.
  auto unmarked = captured;
  unmarked.erase(unmarked.begin() + 12);
  auto envelope = envelopeAt(unmarked, 12U);
  envelope["event_count"] = 1;
  envelope["data_collections"] = Json::array(
      {{{"data_collection", "benchbase.usertable"}, {"event_count", 1}}});
  setEnvelope(unmarked, 12U, envelope);
  renumberStreamIds(unmarked);
  expectError(std::move(unmarked),
              CdcErrorCode::UnmarkedApplicationTransaction);

  auto multiple = captured;
  Json second_marker = multiple.at(15U);
  envelope = Json::parse(second_marker[1][1].get<std::string>());
  envelope["transaction"]["total_order"] = 2;
  envelope["transaction"]["data_collection_order"] = 2;
  second_marker[1][1] = envelope.dump();
  multiple.insert(multiple.begin() + 16, std::move(second_marker));
  envelope = envelopeAt(multiple, 17U);
  envelope["event_count"] = 2;
  envelope["data_collections"][0]["event_count"] = 2;
  setEnvelope(multiple, 17U, envelope);
  renumberStreamIds(multiple);
  expectError(std::move(multiple), CdcErrorCode::MarkerMultiple);

  auto non_final = captured;
  std::swap(non_final[11][1], non_final[12][1]);
  envelope = envelopeAt(non_final, 11U);
  envelope["transaction"]["total_order"] = 1;
  setEnvelope(non_final, 11U, envelope);
  envelope = envelopeAt(non_final, 12U);
  envelope["transaction"]["total_order"] = 2;
  setEnvelope(non_final, 12U, envelope);
  renumberStreamIds(non_final);
  expectError(std::move(non_final), CdcErrorCode::MarkerNotLast);

  auto malformed = captured;
  envelope = envelopeAt(malformed, 12U);
  envelope["after"]["txn_id"] = 0;
  setEnvelope(malformed, 12U, envelope);
  expectError(std::move(malformed), CdcErrorCode::MarkerMalformed);

  auto missing_before = captured;
  envelope = envelopeAt(missing_before, 15U);
  envelope["before"] = nullptr;
  setEnvelope(missing_before, 15U, envelope);
  expectError(std::move(missing_before),
              CdcErrorCode::MarkerBeforeImageMissing);

  auto broken_chain = captured;
  envelope = envelopeAt(broken_chain, 15U);
  envelope["before"]["txn_id"] = 99;
  setEnvelope(broken_chain, 15U, envelope);
  expectError(std::move(broken_chain), CdcErrorCode::MarkerChainMismatch);

  auto duplicate_marker = captured;
  envelope = envelopeAt(duplicate_marker, 15U);
  envelope["after"]["txn_id"] = 1;
  setEnvelope(duplicate_marker, 15U, envelope);
  expectError(std::move(duplicate_marker), CdcErrorCode::MarkerDuplicate);

  auto update_as_first = captured;
  envelope = envelopeAt(update_as_first, 12U);
  envelope["op"] = "u";
  envelope["before"] = Json{{"session_id", "ycsb/worker/1"}, {"txn_id", 0}};
  setEnvelope(update_as_first, 12U, envelope);
  expectError(std::move(update_as_first), CdcErrorCode::MarkerChainMismatch);

  auto marker_during_bootstrap =
      selectEntries(captured, {0U, 1U, 2U, 14U, 15U, 16U});
  expectError(std::move(marker_during_bootstrap),
              CdcErrorCode::ExternalWriterObserved);

  auto before_run = selectEntries(captured, {3U, 4U, 5U, 6U});
  expectError(std::move(before_run), CdcErrorCode::FenceOrderInvalid);

  auto after_end = captured;
  for (const auto index : {14U, 15U, 16U}) {
    Json cloned = captured.at(index);
    rewriteStructuralId(cloned, "0-1-99");
    after_end.push_back(std::move(cloned));
  }
  renumberStreamIds(after_end);
  expectError(std::move(after_end), CdcErrorCode::CdcApplicationAfterEnd);

  auto duplicate_fence = captured;
  for (const auto index : {42U, 43U, 44U}) {
    Json cloned = captured.at(index);
    rewriteStructuralId(cloned, "0-1-100");
    duplicate_fence.push_back(std::move(cloned));
  }
  renumberStreamIds(duplicate_fence);
  expectError(std::move(duplicate_fence), CdcErrorCode::FenceDuplicate);

  auto fence_out_of_order =
      selectEntries(captured, {0U, 1U, 2U, 42U, 43U, 44U});
  expectError(std::move(fence_out_of_order), CdcErrorCode::FenceOrderInvalid);

  auto wrong_run = captured;
  auto key = keyAt(wrong_run, 1U);
  envelope = envelopeAt(wrong_run, 1U);
  key["run_id"] = "00112233-4455-6677-8899-aabbccddeeff";
  envelope["after"]["run_id"] = "00112233-4455-6677-8899-aabbccddeeff";
  setKey(wrong_run, 1U, key);
  setEnvelope(wrong_run, 1U, envelope);
  expectError(std::move(wrong_run), CdcErrorCode::RunIdMismatch);
}

void checkNormalizationFailuresAndPrimaryKeyUpdateRule() {
  const auto captured = loadFixture();

  auto missing_key = captured;
  auto envelope = envelopeAt(missing_key, 11U);
  envelope["after"].erase("ycsb_key");
  setEnvelope(missing_key, 11U, envelope);
  expectError(std::move(missing_key), CdcErrorCode::PrimaryKeyImageIncomplete);

  auto incomplete_value = captured;
  envelope = envelopeAt(incomplete_value, 11U);
  envelope["after"].erase("field10");
  setEnvelope(incomplete_value, 11U, envelope);
  expectError(std::move(incomplete_value), CdcErrorCode::CdcRecordMalformed);

  auto key_disagrees = captured;
  auto key = keyAt(key_disagrees, 11U);
  key["ycsb_key"] = 43;
  setKey(key_disagrees, 11U, key);
  expectError(std::move(key_disagrees),
              CdcErrorCode::PrimaryKeyImageIncomplete);

  auto invalid_field_type = captured;
  envelope = envelopeAt(invalid_field_type, 11U);
  envelope["after"]["field1"] = 7;
  setEnvelope(invalid_field_type, 11U, envelope);
  expectError(std::move(invalid_field_type), CdcErrorCode::CdcRecordMalformed);

  auto mismatched_before_image = captured;
  envelope = envelopeAt(mismatched_before_image, 20U);
  envelope["before"]["field1"] = "does-not-match-prior-after";
  setEnvelope(mismatched_before_image, 20U, envelope);
  expectError(std::move(mismatched_before_image),
              CdcErrorCode::CdcRecordMalformed);

  auto mismatched_before_key = captured;
  envelope = envelopeAt(mismatched_before_key, 20U);
  envelope["before"]["ycsb_key"] = 101;
  setEnvelope(mismatched_before_key, 20U, envelope);
  expectError(std::move(mismatched_before_key),
              CdcErrorCode::CdcRecordMalformed);

  auto repeated_insert = captured;
  envelope = envelopeAt(repeated_insert, 19U);
  envelope["op"] = "c";
  envelope["before"] = nullptr;
  setEnvelope(repeated_insert, 19U, envelope);
  expectError(std::move(repeated_insert), CdcErrorCode::CdcRecordMalformed);

  auto repeated_delete = captured;
  envelope = envelopeAt(repeated_delete, 20U);
  envelope["op"] = "d";
  envelope["after"] = nullptr;
  setEnvelope(repeated_delete, 20U, envelope);
  expectError(std::move(repeated_delete), CdcErrorCode::CdcRecordMalformed);

  auto update_after_delete = captured;
  auto delete_envelope = envelopeAt(update_after_delete, 20U);
  delete_envelope["op"] = "d";
  delete_envelope["after"] = nullptr;
  setEnvelope(update_after_delete, 20U, delete_envelope);
  envelope = envelopeAt(update_after_delete, 21U);
  envelope["op"] = "u";
  envelope["before"] = delete_envelope["before"];
  envelope["after"] = delete_envelope["before"];
  envelope["after"]["field1"] = "after-delete";
  setEnvelope(update_after_delete, 21U, envelope);
  expectError(std::move(update_after_delete), CdcErrorCode::CdcRecordMalformed);

  auto oversized_unsigned_key = captured;
  const auto maximum_unsigned = std::numeric_limits<std::uint64_t>::max();
  key = keyAt(oversized_unsigned_key, 11U);
  envelope = envelopeAt(oversized_unsigned_key, 11U);
  key["ycsb_key"] = maximum_unsigned;
  envelope["before"]["ycsb_key"] = maximum_unsigned;
  envelope["after"]["ycsb_key"] = maximum_unsigned;
  setKey(oversized_unsigned_key, 11U, key);
  setEnvelope(oversized_unsigned_key, 11U, envelope);
  expectError(std::move(oversized_unsigned_key),
              CdcErrorCode::PrimaryKeyImageIncomplete);

  // Derive the connector's alternate single-update representation from the
  // captured delete+insert PK change and verify the mandated two-write result.
  auto primary_key_update = captured;
  auto update_envelope = envelopeAt(primary_key_update, 38U);
  const auto insert_envelope = envelopeAt(primary_key_update, 39U);
  update_envelope["op"] = "u";
  update_envelope["after"] = insert_envelope["after"];
  setKey(primary_key_update, 38U, keyAt(primary_key_update, 39U));
  setEnvelope(primary_key_update, 38U, update_envelope);
  primary_key_update.erase(primary_key_update.begin() + 39);
  envelope = envelopeAt(primary_key_update, 39U);
  envelope["transaction"]["total_order"] = 2;
  setEnvelope(primary_key_update, 39U, envelope);
  envelope = envelopeAt(primary_key_update, 40U);
  envelope["event_count"] = 2;
  envelope["data_collections"][0]["event_count"] = 1;
  setEnvelope(primary_key_update, 40U, envelope);
  renumberStreamIds(primary_key_update);
  const auto transactions = replay(primary_key_update);
  BLACKBOX_REQUIRE(transactions.size() > 9U);
  BLACKBOX_REQUIRE(transactions[9].writes.size() == 2U);
  BLACKBOX_CHECK(transactions[9].writes[0].key ==
                 CanonicalYcsbCodec::encodeKey("benchbase", "usertable", -2));
  BLACKBOX_CHECK(transactions[9].writes[0].state ==
                 NormalizedValueState::Present);
  BLACKBOX_CHECK(transactions[9].writes[1].key ==
                 CanonicalYcsbCodec::encodeKey("benchbase", "usertable", -1));
  BLACKBOX_CHECK(transactions[9].writes[1].state ==
                 NormalizedValueState::Absent);
}

void checkAttemptGapsAndContiguousFrames() {
  const auto captured = loadFixture();

  // Aborted physical attempts may leave a committed marker-ID gap.
  auto marker_gap =
      selectEntries(captured, {0U, 1U, 2U, 3U, 4U, 5U, 6U, 7U, 8U, 9U, 10U, 11U,
                               12U, 13U, 14U, 15U, 16U});
  auto envelope = envelopeAt(marker_gap, 15U);
  envelope["after"]["txn_id"] = 3;
  setEnvelope(marker_gap, 15U, envelope);
  const auto gap_transactions = replay(marker_gap);
  BLACKBOX_REQUIRE(gap_transactions.size() == 5U);
  BLACKBOX_REQUIRE(gap_transactions[4].correlation_id.has_value());
  BLACKBOX_CHECK(gap_transactions[4].correlation_id->txn_id == 3U);

  // MariaDB transaction metadata and rows form one contiguous frame. A second
  // BEGIN before the active transaction's END is invalid even when it names a
  // distinct connector transaction ID.
  Json interleaved = Json::array();
  interleaved.push_back(captured[0]);
  interleaved.push_back(captured[1]);
  interleaved.push_back(captured[2]);
  interleaved.push_back(captured[3]); // BEGIN loader A
  interleaved.push_back(captured[4]); // row A (key 42)

  Json begin_b = captured[3];
  rewriteStructuralId(begin_b, "0-1-200");
  interleaved.push_back(std::move(begin_b));
  renumberStreamIds(interleaved);
  expectError(std::move(interleaved), CdcErrorCode::CdcOrderInvalid);
}

void checkCapturedTpccTransactionsAndFailures() {
  auto fixture = loadTpccTransactionFixture();
  BLACKBOX_CHECK(fixture.at("backend") == "mariadb");
  BLACKBOX_CHECK(fixture.at("mariadb_image") == "mariadb:12.3");
  BLACKBOX_REQUIRE(fixture.at("startup_schema").size() == 26U);
  BLACKBOX_REQUIRE(fixture.at("transactions").size() == 12U);

  DebeziumTxnAssembler assembler(tpccAssemblerConfig(fixture));
  std::vector<CdcTransaction> actual;
  for (const auto &record : recordsFromFixture(tpccRawEntries(fixture))) {
    auto completed = assembler.accept(record);
    if (completed.has_value()) {
      actual.push_back(std::move(*completed));
    }
  }
  assembler.finish();
  BLACKBOX_REQUIRE(actual.size() == fixture.at("transactions").size());
  std::set<std::string> tables;
  std::uint64_t expected_write_count = 0U;
  for (std::size_t index = 0; index < actual.size(); ++index) {
    const auto &expected = fixture.at("transactions")[index].at("expected");
    const auto &transaction = actual[index];
    BLACKBOX_CHECK(transaction.cdc_seq == expected.at("cdc_seq"));
    BLACKBOX_CHECK(blackbox::toString(transaction.kind) ==
                   expected.at("kind").get<std::string>());
    if (expected.at("fence_phase").is_null()) {
      BLACKBOX_CHECK(!transaction.fence_phase.has_value());
    } else {
      BLACKBOX_REQUIRE(transaction.fence_phase.has_value());
      BLACKBOX_CHECK(blackbox::toString(*transaction.fence_phase) ==
                     expected.at("fence_phase").get<std::string>());
    }
    if (expected.at("correlation_id").is_null()) {
      BLACKBOX_CHECK(!transaction.correlation_id.has_value());
    } else {
      BLACKBOX_REQUIRE(transaction.correlation_id.has_value());
      BLACKBOX_CHECK(
          transaction.correlation_id->run_id ==
          expected.at("correlation_id").at("run_id").get<std::string>());
      BLACKBOX_CHECK(
          transaction.correlation_id->session_id ==
          expected.at("correlation_id").at("session_id").get<std::string>());
      BLACKBOX_CHECK(
          transaction.correlation_id->txn_id ==
          expected.at("correlation_id").at("txn_id").get<std::uint64_t>());
    }
    const auto &expected_writes = expected.at("writes");
    BLACKBOX_REQUIRE(transaction.writes.size() == expected_writes.size());
    expected_write_count += expected_writes.size();
    for (std::size_t write_index = 0; write_index < transaction.writes.size();
         ++write_index) {
      const auto &write = transaction.writes[write_index];
      const auto &expected_write = expected_writes[write_index];
      BLACKBOX_CHECK(bytesHex(write.key) ==
                     expected_write.at("key_hex").get<std::string>());
      BLACKBOX_CHECK(blackbox::toString(write.state) ==
                     expected_write.at("state").get<std::string>());
      if (write.state == NormalizedValueState::Present) {
        BLACKBOX_REQUIRE(write.value.has_value());
        BLACKBOX_CHECK(bytesHex(*write.value) ==
                       expected_write.at("value_hex").get<std::string>());
      } else {
        BLACKBOX_CHECK(!write.value.has_value());
        BLACKBOX_CHECK(!expected_write.contains("value_hex"));
      }
      tables.insert(expected_write.at("table").get<std::string>());
    }
  }
  const auto metrics = assembler.metrics();
  BLACKBOX_CHECK(metrics.completed_transactions == 12U);
  BLACKBOX_CHECK(metrics.completed_run_control_transactions == 3U);
  BLACKBOX_CHECK(metrics.completed_bootstrap_transactions == 1U);
  BLACKBOX_CHECK(metrics.completed_workload_transactions == 8U);
  BLACKBOX_CHECK(metrics.normalized_write_events == expected_write_count);
  BLACKBOX_CHECK(
      tables.size() ==
      blackbox::BenchmarkAdapterRegistry::require("tpcc").tables().size());
  const auto order_status = std::find_if(
      fixture.at("transactions").begin(), fixture.at("transactions").end(),
      [](const Json &transaction) {
        return transaction.at("name") == "order_status_marker_only";
      });
  BLACKBOX_REQUIRE(order_status != fixture.at("transactions").end());
  const auto order_status_index = static_cast<std::size_t>(
      std::distance(fixture.at("transactions").begin(), order_status));
  BLACKBOX_REQUIRE(order_status_index < actual.size());
  BLACKBOX_CHECK(actual[order_status_index].kind ==
                 CdcTransactionKind::WorkloadMarked);
  BLACKBOX_REQUIRE(actual[order_status_index].correlation_id.has_value());
  BLACKBOX_CHECK(actual[order_status_index].correlation_id->txn_id == 4U);
  BLACKBOX_CHECK(actual[order_status_index].writes.empty());
  BLACKBOX_CHECK(order_status->at("raw_entries").size() == 3U);

  const auto payment = std::find_if(
      fixture.at("transactions").begin(), fixture.at("transactions").end(),
      [](const Json &transaction) {
        return transaction.at("name") == "payment_shape";
      });
  BLACKBOX_REQUIRE(payment != fixture.at("transactions").end());
  const auto payment_index = static_cast<std::size_t>(
      std::distance(fixture.at("transactions").begin(), payment));
  BLACKBOX_REQUIRE(payment_index < actual.size());
  const auto payment_raw_row_count = [&](const std::string_view table,
                                         const std::string_view operation) {
    return static_cast<std::size_t>(
        std::count_if(payment->at("raw_entries").begin(),
                      payment->at("raw_entries").end(), [&](const Json &entry) {
                        const auto envelope = entryEnvelope(entry);
                        return envelope.contains("op") &&
                               envelope.at("source").at("table") == table &&
                               envelope.at("op") == operation;
                      }));
  };
  BLACKBOX_CHECK(payment_raw_row_count("warehouse", "u") == 1U);
  BLACKBOX_CHECK(payment_raw_row_count("district", "u") == 1U);
  BLACKBOX_CHECK(payment_raw_row_count("customer", "u") == 1U);
  BLACKBOX_CHECK(payment_raw_row_count("history", "c") == 1U);
  BLACKBOX_CHECK(payment_raw_row_count("session_txn", "c") == 1U);
  BLACKBOX_REQUIRE(actual[payment_index].writes.size() == 4U);
  for (const auto table : {"benchbase.warehouse", "benchbase.district",
                           "benchbase.customer", "benchbase.history"}) {
    BLACKBOX_CHECK(std::count_if(payment->at("expected").at("writes").begin(),
                                 payment->at("expected").at("writes").end(),
                                 [&](const Json &write) {
                                   return write.at("table") == table;
                                 }) == 1);
  }

  const auto bootstrap_history =
      entryEnvelope(tpccRowEntry(fixture, "bootstrap_rows", "history"));
  const auto payment_history =
      entryEnvelope(tpccRowEntry(fixture, "payment_shape", "history"));
  BLACKBOX_CHECK(bootstrap_history.at("after").at("h_id") == 1);
  BLACKBOX_CHECK(payment_history.at("after").at("h_id") == 2);
  BLACKBOX_CHECK(payment_history.at("after").at("h_c_w_id") ==
                 payment_history.at("after").at("h_w_id"));
  const auto bootstrap_history_row = blackbox::CanonicalTpccCodec::encodeRow(
      "benchbase", "history", bootstrap_history.at("after"));
  const auto payment_history_row = blackbox::CanonicalTpccCodec::encodeRow(
      "benchbase", "history", payment_history.at("after"));
  BLACKBOX_CHECK(bootstrap_history_row.key != payment_history_row.key);
  const Json expected_history_key{{"h_id", 2}};
  BLACKBOX_CHECK(entryKey(tpccRowEntry(fixture, "payment_shape", "history")) ==
                 expected_history_key);
  const auto history_write = std::find_if(
      actual[payment_index].writes.begin(), actual[payment_index].writes.end(),
      [&](const auto &write) { return write.key == payment_history_row.key; });
  BLACKBOX_REQUIRE(history_write != actual[payment_index].writes.end());
  BLACKBOX_CHECK(history_write->state == NormalizedValueState::Present);
  BLACKBOX_REQUIRE(history_write->value.has_value());
  BLACKBOX_CHECK(*history_write->value == payment_history_row.value);

  const auto *history_contract =
      blackbox::BenchmarkAdapterRegistry::require("tpcc").table(
          "benchbase.history");
  BLACKBOX_REQUIRE(history_contract != nullptr);
  BLACKBOX_REQUIRE(history_contract->primary_key.size() == 1U);
  BLACKBOX_CHECK(history_contract->primary_key[0] == "h_id");
  BLACKBOX_REQUIRE(!history_contract->columns.empty());
  BLACKBOX_CHECK(history_contract->columns[0].name == "h_id");
  BLACKBOX_REQUIRE(history_contract->columns[0].generated.has_value());
  BLACKBOX_CHECK(*history_contract->columns[0].generated == "auto_increment");

  const auto new_order = std::find_if(
      fixture.at("transactions").begin(), fixture.at("transactions").end(),
      [](const Json &transaction) {
        return transaction.at("name") == "new_order_shape";
      });
  BLACKBOX_REQUIRE(new_order != fixture.at("transactions").end());
  const auto new_order_index = static_cast<std::size_t>(
      std::distance(fixture.at("transactions").begin(), new_order));
  BLACKBOX_REQUIRE(new_order_index < actual.size());
  const auto raw_row_count = [&](const std::string_view table,
                                 const std::string_view operation) {
    return static_cast<std::size_t>(std::count_if(
        new_order->at("raw_entries").begin(),
        new_order->at("raw_entries").end(), [&](const Json &entry) {
          const auto envelope = entryEnvelope(entry);
          return envelope.contains("op") &&
                 envelope.at("source").at("table") == table &&
                 envelope.at("op") == operation;
        }));
  };
  BLACKBOX_CHECK(raw_row_count("stock", "u") == 2U);
  BLACKBOX_CHECK(raw_row_count("district", "u") == 1U);
  BLACKBOX_CHECK(raw_row_count("oorder", "c") == 1U);
  BLACKBOX_CHECK(raw_row_count("new_order", "c") == 1U);
  BLACKBOX_CHECK(raw_row_count("order_line", "c") == 2U);
  BLACKBOX_CHECK(raw_row_count("session_txn", "u") == 1U);

  const auto expected_table_count = [&](const std::string_view table) {
    return static_cast<std::size_t>(std::count_if(
        new_order->at("expected").at("writes").begin(),
        new_order->at("expected").at("writes").end(),
        [&](const Json &write) { return write.at("table") == table; }));
  };
  BLACKBOX_REQUIRE(actual[new_order_index].writes.size() == 6U);
  BLACKBOX_CHECK(expected_table_count("benchbase.stock") == 1U);
  BLACKBOX_CHECK(expected_table_count("benchbase.district") == 1U);
  BLACKBOX_CHECK(expected_table_count("benchbase.oorder") == 1U);
  BLACKBOX_CHECK(expected_table_count("benchbase.new_order") == 1U);
  BLACKBOX_CHECK(expected_table_count("benchbase.order_line") == 2U);
  BLACKBOX_CHECK(expected_table_count("benchbase.session_txn") == 0U);

  const auto first_stock =
      entryEnvelope(tpccRowEntry(fixture, "new_order_shape", "stock", 0U));
  const auto second_stock =
      entryEnvelope(tpccRowEntry(fixture, "new_order_shape", "stock", 1U));
  BLACKBOX_CHECK(first_stock.at("after") == second_stock.at("before"));
  BLACKBOX_CHECK(first_stock.at("before") == second_stock.at("after"));
  const auto final_stock = blackbox::CanonicalTpccCodec::encodeRow(
      "benchbase", "stock", second_stock.at("after"));
  const auto stock_write = std::find_if(
      actual[new_order_index].writes.begin(),
      actual[new_order_index].writes.end(),
      [&](const auto &write) { return write.key == final_stock.key; });
  BLACKBOX_REQUIRE(stock_write != actual[new_order_index].writes.end());
  BLACKBOX_CHECK(stock_write->state == NormalizedValueState::Present);
  BLACKBOX_REQUIRE(stock_write->value.has_value());
  BLACKBOX_CHECK(*stock_write->value == final_stock.value);

  const auto empty_new_order = std::find_if(
      new_order->at("expected").at("writes").begin(),
      new_order->at("expected").at("writes").end(), [](const Json &write) {
        return write.at("table") == "benchbase.new_order";
      });
  BLACKBOX_REQUIRE(empty_new_order !=
                   new_order->at("expected").at("writes").end());
  BLACKBOX_CHECK(empty_new_order->at("value_hex") == "");

  {
    auto invalid = fixture;
    auto &entry = tpccRowEntry(invalid, "payment_shape", "warehouse");
    auto envelope = entryEnvelope(entry);
    envelope["source"]["table"] = "unknown_table";
    setEntryEnvelope(entry, envelope);
    expectError(tpccRawEntries(invalid), CdcErrorCode::UnexpectedTable,
                tpccAssemblerConfig(invalid), false);
  }
  {
    auto invalid = fixture;
    auto &entry = tpccRowEntry(invalid, "payment_shape", "warehouse");
    auto key = entryKey(entry);
    key.erase("w_id");
    setEntryKey(entry, key);
    expectError(tpccRawEntries(invalid),
                CdcErrorCode::PrimaryKeyImageIncomplete,
                tpccAssemblerConfig(invalid), false);
  }
  {
    auto invalid = fixture;
    auto &entry = tpccRowEntry(invalid, "payment_shape", "warehouse");
    auto envelope = entryEnvelope(entry);
    envelope["after"]["w_id"] = nullptr;
    setEntryEnvelope(entry, envelope);
    expectError(tpccRawEntries(invalid),
                CdcErrorCode::PrimaryKeyImageIncomplete,
                tpccAssemblerConfig(invalid), false);
  }
  {
    auto invalid = fixture;
    auto &entry = tpccRowEntry(invalid, "payment_shape", "warehouse");
    auto key = entryKey(entry);
    key["w_id"] = 2;
    setEntryKey(entry, key);
    expectError(tpccRawEntries(invalid),
                CdcErrorCode::PrimaryKeyImageIncomplete,
                tpccAssemblerConfig(invalid), false);
  }
  {
    auto invalid = fixture;
    auto &entry = tpccRowEntry(invalid, "net_no_change", "item", 1U);
    auto envelope = entryEnvelope(entry);
    envelope["before"]["i_name"] = "broken-chain";
    setEntryEnvelope(entry, envelope);
    expectError(tpccRawEntries(invalid), CdcErrorCode::CdcRecordMalformed,
                tpccAssemblerConfig(invalid), false);
  }
  {
    auto invalid = fixture;
    auto &entry = tpccRowEntry(invalid, "delivery_shape", "new_order");
    auto envelope = entryEnvelope(entry);
    envelope["before"] = nullptr;
    setEntryEnvelope(entry, envelope);
    expectError(tpccRawEntries(invalid), CdcErrorCode::CdcRecordMalformed,
                tpccAssemblerConfig(invalid), false);
  }
  {
    auto invalid = fixture;
    auto &entry = tpccRowEntry(invalid, "payment_shape", "warehouse");
    auto envelope = entryEnvelope(entry);
    envelope["after"]["w_ytd"] = "not-a-decimal";
    setEntryEnvelope(entry, envelope);
    expectError(tpccRawEntries(invalid), CdcErrorCode::CanonicalEncodingFailed,
                tpccAssemblerConfig(invalid), false);
  }
  {
    auto invalid = fixture;
    auto &entry = tpccRowEntry(invalid, "payment_shape", "history");
    auto key = entryKey(entry);
    key.erase("h_id");
    setEntryKey(entry, key);
    expectError(tpccRawEntries(invalid),
                CdcErrorCode::PrimaryKeyImageIncomplete,
                tpccAssemblerConfig(invalid), false);
  }
  {
    auto invalid = fixture;
    auto &entry = tpccRowEntry(invalid, "payment_shape", "history");
    auto envelope = entryEnvelope(entry);
    envelope["after"]["h_id"] = nullptr;
    setEntryEnvelope(entry, envelope);
    expectError(tpccRawEntries(invalid),
                CdcErrorCode::PrimaryKeyImageIncomplete,
                tpccAssemblerConfig(invalid), false);
  }
  {
    auto invalid = fixture;
    auto &entry = tpccRowEntry(invalid, "payment_shape", "history");
    auto key = entryKey(entry);
    key["h_id"] = 1;
    setEntryKey(entry, key);
    expectError(tpccRawEntries(invalid),
                CdcErrorCode::PrimaryKeyImageIncomplete,
                tpccAssemblerConfig(invalid), false);
  }
  {
    auto invalid = fixture;
    auto &entry = tpccRowEntry(invalid, "payment_shape", "history");
    auto envelope = entryEnvelope(entry);
    envelope["after"]["h_amount"] = "not-a-decimal";
    setEntryEnvelope(entry, envelope);
    expectError(tpccRawEntries(invalid), CdcErrorCode::CanonicalEncodingFailed,
                tpccAssemblerConfig(invalid), false);
  }
  {
    auto invalid = fixture;
    auto &entry = tpccRowEntry(invalid, "payment_shape", "customer");
    auto envelope = entryEnvelope(entry);
    envelope["after"].erase("c_data");
    setEntryEnvelope(entry, envelope);
    expectError(tpccRawEntries(invalid), CdcErrorCode::CdcRecordMalformed,
                tpccAssemblerConfig(invalid), false);
  }
  {
    auto invalid = fixture;
    auto &entry = tpccRowEntry(invalid, "payment_shape", "customer");
    auto envelope = entryEnvelope(entry);
    envelope["after"]["c_ytd_payment"] = "not-a-float";
    setEntryEnvelope(entry, envelope);
    expectError(tpccRawEntries(invalid), CdcErrorCode::CanonicalEncodingFailed,
                tpccAssemblerConfig(invalid), false);
  }
  {
    auto invalid = fixture;
    auto &entry = tpccRowEntry(invalid, "new_order_shape", "stock");
    auto key = entryKey(entry);
    key.erase("s_i_id");
    setEntryKey(entry, key);
    expectError(tpccRawEntries(invalid),
                CdcErrorCode::PrimaryKeyImageIncomplete,
                tpccAssemblerConfig(invalid), false);
  }
  {
    auto invalid = fixture;
    auto &entry = tpccRowEntry(invalid, "new_order_shape", "stock");
    auto envelope = entryEnvelope(entry);
    envelope["after"].erase("s_quantity");
    setEntryEnvelope(entry, envelope);
    expectError(tpccRawEntries(invalid), CdcErrorCode::CdcRecordMalformed,
                tpccAssemblerConfig(invalid), false);
  }
  {
    auto invalid = fixture;
    auto &entry = tpccRowEntry(invalid, "new_order_shape", "stock", 1U);
    auto envelope = entryEnvelope(entry);
    envelope["before"]["s_quantity"] = 40;
    setEntryEnvelope(entry, envelope);
    expectError(tpccRawEntries(invalid), CdcErrorCode::CdcRecordMalformed,
                tpccAssemblerConfig(invalid), false);
  }
  {
    auto invalid = fixture;
    auto &entry = tpccRowEntry(invalid, "new_order_shape", "order_line");
    auto envelope = entryEnvelope(entry);
    envelope["after"]["ol_amount"] = "not-a-decimal";
    setEntryEnvelope(entry, envelope);
    expectError(tpccRawEntries(invalid), CdcErrorCode::CanonicalEncodingFailed,
                tpccAssemblerConfig(invalid), false);
  }
}

} // namespace

int runDebeziumAssemblerTests() {
  checkCapturedStartupSchemaSnapshot();
  checkCapturedLifecycleAndNormalization();
  checkMeasurementMetrics();
  checkStructuralFailuresAreClosed();
  checkMarkerAndFenceFailuresAreClosed();
  checkNormalizationFailuresAndPrimaryKeyUpdateRule();
  checkAttemptGapsAndContiguousFrames();
  checkCapturedTpccTransactionsAndFailures();
  return 0;
}
