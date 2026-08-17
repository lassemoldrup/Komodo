#include "blackbox/canonical_ycsb_codec.h"
#include "blackbox/cdc_error.h"
#include "blackbox/cdc_types.h"
#include "blackbox/debezium_assembler.h"
#include "test_support.h"

#include <nlohmann/json.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <functional>
#include <initializer_list>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
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

constexpr std::string_view kFixtureRunId =
    "084cc5a1-3cbe-4fef-bb5c-43559a1c13a0";

[[nodiscard]] DebeziumAssemblerConfig assemblerConfig() {
  return DebeziumAssemblerConfig{
      .run_id = std::string(kFixtureRunId),
      .backend_source_id = "mariadb-main",
      .debezium_version = "3.6.1.Final",
      .application_tables = {"benchbase.usertable"},
      .transaction_marker_table = "benchbase.session_txn",
      .run_fence_table = "benchbase.checker_run_fence",
  };
}

[[nodiscard]] Json loadFixture() {
  std::ifstream input(std::string(BLACKBOX_CDC_FIXTURE_DIR) +
                      "/mariadb_debezium_3_6_1_lifecycle.json");
  if (!input) {
    throw std::runtime_error("failed to open captured MariaDB CDC fixture");
  }
  return Json::parse(input);
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
                 DebeziumAssemblerConfig config = assemblerConfig()) {
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
replay(const Json &root, DebeziumAssemblerConfig config = assemblerConfig()) {
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

  DebeziumTxnAssembler sealed(assemblerConfig());
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

void checkAttemptGapsAndEndCompletionOrder() {
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

  // Derive two interleaved bootstrap assemblies from the two captured loader
  // rows. The transaction whose END arrives first owns the earlier cdc_seq;
  // BEGIN order and the structural IDs have no correlation meaning.
  Json interleaved = Json::array();
  interleaved.push_back(captured[0]);
  interleaved.push_back(captured[1]);
  interleaved.push_back(captured[2]);
  interleaved.push_back(captured[3]); // BEGIN loader A
  interleaved.push_back(captured[4]); // row A (key 42)

  Json begin_b = captured[3];
  rewriteStructuralId(begin_b, "0-1-200");
  interleaved.push_back(std::move(begin_b));
  Json row_b = captured[5];
  rewriteStructuralId(row_b, "0-1-200");
  envelope = Json::parse(row_b[1][1].get<std::string>());
  envelope["transaction"]["total_order"] = 1;
  envelope["transaction"]["data_collection_order"] = 1;
  row_b[1][1] = envelope.dump();
  interleaved.push_back(std::move(row_b));
  Json end_b = captured[6];
  rewriteStructuralId(end_b, "0-1-200");
  envelope = Json::parse(end_b[1][1].get<std::string>());
  envelope["event_count"] = 1;
  envelope["data_collections"][0]["event_count"] = 1;
  end_b[1][1] = envelope.dump();
  interleaved.push_back(std::move(end_b));
  Json end_a = captured[6];
  envelope = Json::parse(end_a[1][1].get<std::string>());
  envelope["event_count"] = 1;
  envelope["data_collections"][0]["event_count"] = 1;
  end_a[1][1] = envelope.dump();
  interleaved.push_back(std::move(end_a));
  renumberStreamIds(interleaved);

  const auto completed = replay(interleaved);
  BLACKBOX_REQUIRE(completed.size() == 3U);
  BLACKBOX_REQUIRE(completed[1].writes.size() == 1U);
  BLACKBOX_REQUIRE(completed[2].writes.size() == 1U);
  BLACKBOX_CHECK(completed[1].cdc_seq == 1U);
  BLACKBOX_CHECK(completed[1].writes[0].key ==
                 CanonicalYcsbCodec::encodeKey("benchbase", "usertable", -1));
  BLACKBOX_CHECK(completed[2].cdc_seq == 2U);
  BLACKBOX_CHECK(completed[2].writes[0].key ==
                 CanonicalYcsbCodec::encodeKey("benchbase", "usertable", 42));

  auto one_active = assemblerConfig();
  one_active.max_active_transactions = 1U;
  expectError(std::move(interleaved), CdcErrorCode::ResourceExhausted,
              std::move(one_active));
}

} // namespace

int runDebeziumAssemblerTests() {
  checkCapturedLifecycleAndNormalization();
  checkStructuralFailuresAreClosed();
  checkMarkerAndFenceFailuresAreClosed();
  checkNormalizationFailuresAndPrimaryKeyUpdateRule();
  checkAttemptGapsAndEndCompletionOrder();
  return 0;
}
