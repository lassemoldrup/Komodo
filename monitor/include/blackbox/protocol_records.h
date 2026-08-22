#pragma once

#include "blackbox/monitor_error.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace blackbox {

enum class ProtocolRecordKind {
  RunManifest,
  SchemaAttestation,
  BootstrapManifest,
  ClientTxn,
  SessionDone,
  RunFailed,
};

enum class SchemaAttestationPhase {
  Startup,
  Final,
};

struct ProtocolSchemaColumn {
  std::string name;
  std::string logical_sql_type;
  bool nullable = false;
  bool generated = false;

  bool operator==(const ProtocolSchemaColumn &) const = default;
};

struct ProtocolSchemaTable {
  std::string canonical_table_name;
  std::vector<std::string> primary_key_columns;
  std::vector<ProtocolSchemaColumn> columns;

  bool operator==(const ProtocolSchemaTable &) const = default;
};

struct ProtocolBackendFact {
  std::string name;
  std::string canonical_value;

  bool operator==(const ProtocolBackendFact &) const = default;
};

struct SchemaAttestation {
  std::string run_id;
  SchemaAttestationPhase phase = SchemaAttestationPhase::Startup;
  std::string backend;
  std::string backend_source_id;
  std::vector<ProtocolSchemaTable> tables;
  std::vector<ProtocolBackendFact> backend_facts;
  std::string canonical_attestation_digest;

  bool operator==(const SchemaAttestation &) const = default;
};

struct RunManifest {
  std::string run_id;
  std::string backend;
  std::string backend_source_id;
  std::string declared_isolation;
  std::string benchmark;
  std::vector<std::string> expected_session_ids;
  std::vector<std::string> application_table_allowlist;
  std::string transaction_marker_table;
  std::vector<std::string> control_table_allowlist;
  std::string result_scope;

  bool operator==(const RunManifest &) const = default;
};

struct ProtocolTableRowCount {
  std::string table;
  std::uint64_t row_count = 0;

  bool operator==(const ProtocolTableRowCount &) const = default;
};

struct BootstrapManifest {
  std::string run_id;
  std::vector<ProtocolTableRowCount> table_row_counts;
  std::string canonical_state_digest;

  bool operator==(const BootstrapManifest &) const = default;
};

struct SessionDone {
  std::string run_id;
  std::string session_id;
  std::uint64_t final_session_seq = 0;
  std::uint64_t logical_transactions_started = 0;
  std::uint64_t successful_transactions = 0;
  std::uint64_t terminal_aborted_transactions = 0;
  std::uint64_t physical_attempts_started = 0;
  std::uint64_t definitively_aborted_attempts = 0;
  std::uint64_t published_transactions = 0;
  std::uint64_t retry_attempts = 0;
  std::uint64_t ambiguous_attempts = 0;
  std::string rolling_correlation_digest;

  bool operator==(const SessionDone &) const = default;
};

struct RunFailed {
  std::string run_id;
  std::optional<std::string> session_id;
  std::string phase;
  std::string reason_code;
  std::string diagnostic;

  bool operator==(const RunFailed &) const = default;
};

struct SessionMarkerEntry {
  std::uint64_t session_seq = 0;
  std::uint64_t txn_id = 0;

  bool operator==(const SessionMarkerEntry &) const = default;
};

using SessionMarkerDigest = std::array<unsigned char, 32>;

struct ProtocolRecordParserLimits {
  std::size_t max_payload_bytes = 4U * 1024U * 1024U;
  std::size_t max_expected_sessions = 100000U;
  std::size_t max_table_entries = 1024U;
};

class ProtocolRecordParser final {
public:
  explicit ProtocolRecordParser(ProtocolRecordParserLimits limits = {});

  [[nodiscard]] ProtocolRecordKind kind(std::string_view payload) const;
  [[nodiscard]] RunManifest parseRunManifest(std::string_view payload) const;
  [[nodiscard]] SchemaAttestation
  parseSchemaAttestation(std::string_view payload) const;
  [[nodiscard]] BootstrapManifest
  parseBootstrapManifest(std::string_view payload) const;
  [[nodiscard]] SessionDone parseSessionDone(std::string_view payload) const;
  [[nodiscard]] RunFailed parseRunFailed(std::string_view payload) const;

private:
  ProtocolRecordParserLimits limits_;
};

[[nodiscard]] SessionMarkerDigest
sessionMarkerDigestSeed(std::string_view run_id, std::string_view session_id);

[[nodiscard]] SessionMarkerDigest
sessionMarkerDigestStep(const SessionMarkerDigest &previous,
                        SessionMarkerEntry entry);

[[nodiscard]] std::string
sessionMarkerDigestHex(const SessionMarkerDigest &digest);

[[nodiscard]] std::string
computeRollingSessionMarkerDigest(std::string_view run_id,
                                  std::string_view session_id,
                                  std::span<const SessionMarkerEntry> entries);

[[nodiscard]] std::string
computeSchemaAttestationDigest(const SchemaAttestation &attestation);

[[nodiscard]] std::string_view toString(ProtocolRecordKind kind) noexcept;
[[nodiscard]] std::string_view toString(SchemaAttestationPhase phase) noexcept;

} // namespace blackbox
