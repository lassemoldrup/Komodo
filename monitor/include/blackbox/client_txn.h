#pragma once

#include "blackbox/canonical_ycsb_codec.h"
#include "blackbox/cdc_types.h"
#include "blackbox/monitor_error.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace blackbox {

enum class ClientValueState {
  Present,
  Absent,
};

enum class ClientReadSource {
  Snapshot,
  Local,
};

struct ClientRead {
  std::uint32_t event_seq = 0;
  CanonicalBytes key;
  ClientValueState state = ClientValueState::Absent;
  std::optional<CanonicalBytes> value;
  ClientReadSource source = ClientReadSource::Snapshot;

  bool operator==(const ClientRead &) const = default;
};

struct ClientTxn {
  std::string run_id;
  std::string session_id;
  std::uint64_t session_seq = 0;
  std::string logical_txn_id;
  std::string attempt_id;
  std::string txn_type;
  RunScopedCorrelationId correlation_id;
  std::string read_digest;
  std::vector<ClientRead> reads;

  bool operator==(const ClientTxn &) const = default;
};

struct ClientTxnParserLimits {
  std::size_t max_payload_bytes = 64U * 1024U * 1024U;
  std::size_t max_reads = 1000U;
  std::size_t max_key_bytes = 16U * 1024U * 1024U;
  std::size_t max_value_bytes = 32U * 1024U * 1024U;
  std::size_t max_decoded_bytes = 64U * 1024U * 1024U;
};

class ClientTxnParser final {
public:
  ClientTxnParser(std::string run_id,
                  std::vector<std::string> expected_session_ids,
                  std::vector<std::string> application_tables,
                  ClientTxnParserLimits limits = {});

  [[nodiscard]] ClientTxn parse(std::string_view payload) const;

private:
  std::string run_id_;
  std::unordered_set<std::string> expected_session_ids_;
  std::unordered_set<std::string> application_tables_;
  ClientTxnParserLimits limits_;
};

[[nodiscard]] std::string
computeClientReadDigest(std::span<const ClientRead> reads);

[[nodiscard]] std::string_view toString(ClientValueState state) noexcept;
[[nodiscard]] std::string_view toString(ClientReadSource source) noexcept;

} // namespace blackbox
