#include "blackbox/redis_reader.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <climits>
#include <cstdint>
#include <exception>
#include <limits>
#include <list>
#include <memory>
#include <new>
#include <sstream>
#include <utility>

#include <sys/socket.h>

namespace blackbox {
namespace {

constexpr std::size_t kMaxStreamNameBytes = 1024U;
constexpr std::size_t kMaxFieldNameBytes = 1024U;
constexpr std::size_t kMaxRenderedStreamIdBytes = 41U;
constexpr std::size_t kMaxRespNumberBytes = 32U;
constexpr std::size_t kMaxServerErrorBytes = 4096U;
constexpr std::size_t kTransportReadBytes = 4096U;
constexpr std::size_t kStringAllocationOverhead = 2U * sizeof(void *);
constexpr std::size_t kObjectAllocationOverhead = 2U * sizeof(void *);
constexpr std::size_t kListNodeAllocationOverhead = 4U * sizeof(void *);

struct StreamId {
  std::uint64_t milliseconds{};
  std::uint64_t sequence{};

  friend bool operator==(const StreamId &, const StreamId &) = default;

  friend bool operator<(const StreamId &left, const StreamId &right) noexcept {
    return left.milliseconds < right.milliseconds ||
           (left.milliseconds == right.milliseconds &&
            left.sequence < right.sequence);
  }
};

[[nodiscard]] std::string binary_string(const redisReply *reply) {
  if (reply == nullptr || reply->type != REDIS_REPLY_STRING ||
      (reply->str == nullptr && reply->len != 0U)) {
    throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                           "XREAD reply contains a non-bulk-string value");
  }
  if (reply->len == 0U) {
    return {};
  }
  try {
    return std::string(reply->str, reply->len);
  } catch (const std::bad_alloc &) {
    throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                           "XREAD bulk-string allocation failed");
  } catch (const std::length_error &) {
    throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                           "XREAD bulk-string allocation failed");
  }
}

[[nodiscard]] std::size_t binary_size(const redisReply *reply) {
  if (reply == nullptr || reply->type != REDIS_REPLY_STRING ||
      (reply->str == nullptr && reply->len != 0U)) {
    throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                           "XREAD reply contains a non-bulk-string value");
  }
  return reply->len;
}

[[nodiscard]] std::size_t checked_sum(const std::size_t left,
                                      const std::size_t right) {
  if (left > std::numeric_limits<std::size_t>::max() - right) {
    throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                           "XREAD reply byte count overflowed");
  }
  return left + right;
}

[[nodiscard]] std::size_t checked_product(const std::size_t left,
                                          const std::size_t right) {
  if (left != 0U && right > std::numeric_limits<std::size_t>::max() / left) {
    throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                           "XREAD reply byte count overflowed");
  }
  return left * right;
}

[[nodiscard]] bool canonical_decimal(std::string_view value) noexcept {
  if (value.empty()) {
    return false;
  }
  if (value.size() > 1U && value.front() == '0') {
    return false;
  }
  for (const char character : value) {
    if (character < '0' || character > '9') {
      return false;
    }
  }
  return true;
}

[[nodiscard]] std::optional<std::uint64_t>
parse_u64(std::string_view value) noexcept {
  if (!canonical_decimal(value)) {
    return std::nullopt;
  }
  std::uint64_t parsed{};
  const char *const first = value.data();
  const char *const last = first + value.size();
  const auto result = std::from_chars(first, last, parsed, 10);
  if (result.ec != std::errc{} || result.ptr != last) {
    return std::nullopt;
  }
  return parsed;
}

[[nodiscard]] StreamId parse_stream_id(std::string_view value) {
  const std::size_t separator = value.find('-');
  if (separator == std::string_view::npos ||
      value.find('-', separator + 1U) != std::string_view::npos) {
    throw RedisReaderError(RedisReaderErrorCode::InvalidStreamId,
                           "Redis stream ID is not <milliseconds>-<sequence>");
  }
  const auto milliseconds = parse_u64(value.substr(0U, separator));
  const auto sequence = parse_u64(value.substr(separator + 1U));
  if (!milliseconds.has_value() || !sequence.has_value()) {
    throw RedisReaderError(RedisReaderErrorCode::InvalidStreamId,
                           "Redis stream ID is not canonical unsigned decimal");
  }
  return StreamId{*milliseconds, *sequence};
}

[[nodiscard]] std::string describe_stream(std::string_view stream_name) {
  std::ostringstream output;
  output << "stream of " << stream_name.size() << " byte(s)";
  return output.str();
}

void require_array(const redisReply *reply, std::string_view label) {
  if (reply == nullptr || reply->type != REDIS_REPLY_ARRAY ||
      (reply->elements != 0U && reply->element == nullptr)) {
    throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                           std::string(label) + " is not a RESP array");
  }
}

struct ParsedRecord {
  RedisStreamRecord record;
  StreamId parsed_id;
  std::size_t stream_index{};
};

// A string constructed from a bounded RESP bulk normally allocates close to
// size()+1 bytes. Charging twice that amount plus allocator metadata also
// covers capacity rounding and small-string implementations conservatively.
[[nodiscard]] std::size_t retained_string_bytes(const std::size_t length) {
  const auto content = checked_product(checked_sum(length, 1U), 2U);
  return checked_sum(content, kStringAllocationOverhead);
}

[[nodiscard]] std::size_t retained_record_bytes(
    const std::size_t stream_name_bytes, const std::size_t stream_id_bytes,
    const std::size_t field_name_bytes, const std::size_t value_bytes) {
  auto result = checked_sum(sizeof(ParsedRecord), kListNodeAllocationOverhead);
  result = checked_sum(result, retained_string_bytes(stream_name_bytes));
  result = checked_sum(result, retained_string_bytes(stream_id_bytes));
  result = checked_sum(result, retained_string_bytes(field_name_bytes));
  return checked_sum(result, retained_string_bytes(value_bytes));
}

[[nodiscard]] std::size_t retained_buffer_bytes(const std::size_t length) {
  return checked_sum(checked_product(length, 2U), kStringAllocationOverhead);
}

[[nodiscard]] std::optional<std::size_t>
find_stream_index(const std::vector<RedisStreamSpec> &streams,
                  const std::string_view stream_name) noexcept {
  for (std::size_t index = 0; index < streams.size(); ++index) {
    if (streams[index].stream_name == stream_name) {
      return index;
    }
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<std::size_t>
parse_resp_size(const std::string_view value) noexcept {
  const auto parsed = parse_u64(value);
  if (!parsed.has_value() ||
      *parsed >
          static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return std::nullopt;
  }
  return static_cast<std::size_t>(*parsed);
}

void require_no_queued_transport_bytes(redisContext *context) {
  // One command is outstanding at a time, so plaintext bytes already queued
  // after a complete RESP root cannot belong to a valid later response. This
  // immediate check cannot make claims about bytes a peer delays until after
  // delivery; the pinned Redis server and lifecycle provide that trust
  // boundary. TLS contexts are rejected before I/O because SSL-internal
  // buffering cannot be inspected with MSG_PEEK.
  char byte{};
  for (;;) {
    const auto received =
        ::recv(context->fd, &byte, 1U, MSG_PEEK | MSG_DONTWAIT);
    if (received > 0) {
      throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                             "XREAD reply has queued trailing bytes");
    }
    if (received == 0) {
      throw RedisReaderError(
          RedisReaderErrorCode::ConnectionFailed,
          "Redis transport closed immediately after the XREAD reply");
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return;
    }
    throw RedisReaderError(RedisReaderErrorCode::ConnectionFailed,
                           "XREAD trailing-byte check failed");
  }
}

void require_clean_transport_before_command(redisContext *context) {
  char byte{};
  for (;;) {
    const auto received =
        ::recv(context->fd, &byte, 1U, MSG_PEEK | MSG_DONTWAIT);
    if (received > 0) {
      throw RedisReaderError(
          RedisReaderErrorCode::MalformedReply,
          "Redis transport has queued bytes before XREAD was issued");
    }
    if (received == 0) {
      throw RedisReaderError(RedisReaderErrorCode::ConnectionFailed,
                             "Redis transport is closed before XREAD");
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return;
    }
    throw RedisReaderError(RedisReaderErrorCode::ConnectionFailed,
                           "XREAD pre-command transport check failed");
  }
}

// Reads RESP directly through hiredis' transport function in fixed-size
// chunks. Unlike redisGetReply(), it never accumulates an entire bulk value in
// the hiredis reader before the caller has validated the declared length.
class BoundedRespReader {
public:
  explicit BoundedRespReader(redisContext *context) : context_(context) {}

  [[nodiscard]] char read_type() { return read_byte(); }

  [[nodiscard]] std::string read_line(const std::size_t max_bytes,
                                      const std::string_view label) {
    std::string result;
    result.reserve(std::min(max_bytes, kMaxRespNumberBytes));
    for (;;) {
      const char character = read_byte();
      if (character == '\r') {
        if (read_byte() != '\n') {
          throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                                 std::string(label) +
                                     " has an invalid RESP line ending");
        }
        return result;
      }
      if (character == '\n') {
        throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                               std::string(label) +
                                   " has an invalid RESP line ending");
      }
      if (result.size() >= max_bytes) {
        throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                               std::string(label) +
                                   " exceeds the RESP line byte limit");
      }
      result.push_back(character);
    }
  }

  [[nodiscard]] std::size_t read_array_length(const std::string_view label) {
    if (read_type() != '*') {
      throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                             std::string(label) + " is not a RESP array");
    }
    const std::string rendered = read_line(kMaxRespNumberBytes, label);
    const auto parsed = parse_resp_size(rendered);
    if (!parsed.has_value()) {
      throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                             std::string(label) +
                                 " has an invalid RESP array length");
    }
    return *parsed;
  }

  [[nodiscard]] std::size_t read_bulk_length(const std::string_view label) {
    if (read_type() != '$') {
      throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                             std::string(label) + " is not a RESP bulk string");
    }
    const std::string rendered = read_line(kMaxRespNumberBytes, label);
    const auto parsed = parse_resp_size(rendered);
    if (!parsed.has_value()) {
      throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                             std::string(label) +
                                 " has an invalid RESP bulk length");
    }
    return *parsed;
  }

  [[nodiscard]] std::string read_bulk_body(const std::size_t length,
                                           const std::string_view label) {
    std::string result;
    try {
      result.resize(length);
    } catch (const std::bad_alloc &) {
      throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                             std::string(label) +
                                 " could not be allocated within resources");
    } catch (const std::length_error &) {
      throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                             std::string(label) +
                                 " could not be allocated within resources");
    }
    if (length != 0U) {
      read_exact(result.data(), length);
    }
    if (read_byte() != '\r' || read_byte() != '\n') {
      throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                             std::string(label) +
                                 " has an invalid RESP bulk terminator");
    }
    return result;
  }

  [[nodiscard]] bool has_buffered_bytes() const noexcept {
    return position_ != available_;
  }

private:
  [[nodiscard]] char read_byte() {
    if (position_ == available_) {
      fill();
    }
    return buffer_[position_++];
  }

  void read_exact(char *destination, std::size_t remaining) {
    while (remaining != 0U) {
      if (position_ == available_) {
        fill();
      }
      const std::size_t chunk = std::min(remaining, available_ - position_);
      std::copy_n(buffer_.data() + position_, chunk, destination);
      position_ += chunk;
      destination += chunk;
      remaining -= chunk;
    }
  }

  void fill() {
    if (context_->funcs == nullptr || context_->funcs->read == nullptr) {
      throw RedisReaderError(RedisReaderErrorCode::ConnectionFailed,
                             "hiredis context has no readable transport");
    }
    for (;;) {
      const auto received =
          context_->funcs->read(context_, buffer_.data(), buffer_.size());
      if (received > 0) {
        available_ = static_cast<std::size_t>(received);
        position_ = 0U;
        return;
      }
      if (received < 0) {
        const std::string detail = context_->err == 0
                                       ? std::string("no hiredis error detail")
                                       : std::string(context_->errstr);
        const RedisReaderErrorCode code =
            context_->err == REDIS_ERR_OOM
                ? RedisReaderErrorCode::ResourceExhausted
                : RedisReaderErrorCode::ConnectionFailed;
        throw RedisReaderError(code, "XREAD response read failed: " + detail);
      }
      // Hiredis defines zero as recoverable. read_once() admits only blocking
      // contexts, so the transport owns readiness waiting on every retry.
    }
  }

  redisContext *context_;
  std::array<char, kTransportReadBytes> buffer_{};
  std::size_t position_{};
  std::size_t available_{};
};

} // namespace

struct RedisXReadState::CursorState {
  std::string rendered;
  StreamId parsed;
};

struct RedisXReadState::BufferedBatch {
  std::list<ParsedRecord> records;
  std::vector<StreamId> last_ids;
  std::vector<unsigned char> returned_streams;
  std::size_t retained_bytes{};
  std::size_t transient_bytes{};
};

std::string_view redis_reader_error_name(RedisReaderErrorCode code) noexcept {
  switch (code) {
  case RedisReaderErrorCode::InvalidConfiguration:
    return "INVALID_CONFIGURATION";
  case RedisReaderErrorCode::ConnectionFailed:
    return "CONNECTION_FAILED";
  case RedisReaderErrorCode::ServerError:
    return "SERVER_ERROR";
  case RedisReaderErrorCode::MalformedReply:
    return "MALFORMED_REPLY";
  case RedisReaderErrorCode::UnknownStream:
    return "UNKNOWN_STREAM";
  case RedisReaderErrorCode::DuplicateStream:
    return "DUPLICATE_STREAM";
  case RedisReaderErrorCode::InvalidStreamId:
    return "INVALID_STREAM_ID";
  case RedisReaderErrorCode::DuplicateEntry:
    return "DUPLICATE_ENTRY";
  case RedisReaderErrorCode::ReorderedEntry:
    return "REORDERED_ENTRY";
  case RedisReaderErrorCode::FieldContractViolation:
    return "FIELD_CONTRACT_VIOLATION";
  case RedisReaderErrorCode::CallbackRejected:
    return "CALLBACK_REJECTED";
  case RedisReaderErrorCode::CallbackFailed:
    return "CALLBACK_FAILED";
  case RedisReaderErrorCode::ResourceExhausted:
    return "RESOURCE_EXHAUSTED";
  }
  return "UNKNOWN_READER_ERROR";
}

RedisReaderError::RedisReaderError(RedisReaderErrorCode code,
                                   std::string message)
    : std::runtime_error(std::move(message)), code_(code) {}

RedisReaderErrorCode RedisReaderError::code() const noexcept { return code_; }

RedisXReadState::RedisXReadState(std::vector<RedisStreamSpec> streams,
                                 RedisReaderLimits limits)
    : streams_(std::move(streams)), limits_(limits) {
  if (streams_.empty()) {
    throw RedisReaderError(RedisReaderErrorCode::InvalidConfiguration,
                           "at least one Redis stream is required");
  }
  if (limits_.max_records_per_read == 0U || limits_.max_entry_bytes == 0U ||
      limits_.max_batch_bytes == 0U ||
      limits_.max_entry_bytes > limits_.max_batch_bytes) {
    throw RedisReaderError(RedisReaderErrorCode::InvalidConfiguration,
                           "Redis reader limits are invalid");
  }

  try {
    cursors_.reserve(streams_.size());
    for (std::size_t index = 0; index < streams_.size(); ++index) {
      const RedisStreamSpec &stream = streams_[index];
      if (stream.stream_name.empty()) {
        throw RedisReaderError(RedisReaderErrorCode::InvalidConfiguration,
                               "Redis stream name must not be empty");
      }
      if (stream.stream_name.size() > kMaxStreamNameBytes ||
          (stream.required_field_name.has_value() &&
           stream.required_field_name->size() > kMaxFieldNameBytes)) {
        throw RedisReaderError(RedisReaderErrorCode::InvalidConfiguration,
                               "Redis stream or field name is oversized");
      }
      for (std::size_t previous = 0; previous < index; ++previous) {
        if (streams_[previous].stream_name == stream.stream_name) {
          throw RedisReaderError(
              RedisReaderErrorCode::InvalidConfiguration,
              "Redis stream configuration contains a duplicate name");
        }
      }
      try {
        cursors_.push_back(CursorState{stream.initial_cursor,
                                       parse_stream_id(stream.initial_cursor)});
      } catch (const RedisReaderError &) {
        throw RedisReaderError(
            RedisReaderErrorCode::InvalidConfiguration,
            "Redis stream cursor must be an explicit canonical stream ID");
      }
    }
  } catch (const std::bad_alloc &) {
    throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                           "Redis reader configuration allocation failed");
  } catch (const std::length_error &) {
    throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                           "Redis reader configuration allocation failed");
  }
}

RedisXReadState::~RedisXReadState() = default;

const std::vector<RedisStreamSpec> &RedisXReadState::streams() const noexcept {
  return streams_;
}

std::string_view RedisXReadState::cursor(std::string_view stream_name) const {
  for (std::size_t index = 0; index < streams_.size(); ++index) {
    if (streams_[index].stream_name == stream_name) {
      return cursors_[index].rendered;
    }
  }
  throw RedisReaderError(RedisReaderErrorCode::UnknownStream,
                         "cursor requested for an unknown Redis stream");
}

std::vector<std::string> RedisXReadState::xread_streams_and_cursors() const {
  std::vector<std::string> result;
  result.reserve(streams_.size() * 2U);
  for (const RedisStreamSpec &stream : streams_) {
    result.push_back(stream.stream_name);
  }
  for (const CursorState &cursor_state : cursors_) {
    result.push_back(cursor_state.rendered);
  }
  return result;
}

std::unique_ptr<RedisXReadState::BufferedBatch>
RedisXReadState::new_batch() const {
  auto retained = checked_sum(sizeof(BufferedBatch), kObjectAllocationOverhead);
  retained = checked_sum(retained, retained_buffer_bytes(checked_product(
                                       cursors_.size(), sizeof(StreamId))));
  retained =
      checked_sum(retained, retained_buffer_bytes(checked_product(
                                streams_.size(), sizeof(unsigned char))));
  if (retained > limits_.max_batch_bytes) {
    throw RedisReaderError(
        RedisReaderErrorCode::ResourceExhausted,
        "XREAD validation state exceeds the retained batch byte limit");
  }
  try {
    auto batch = std::make_unique<BufferedBatch>();
    batch->last_ids.reserve(cursors_.size());
    for (const CursorState &cursor_state : cursors_) {
      batch->last_ids.push_back(cursor_state.parsed);
    }
    batch->returned_streams.resize(streams_.size(), 0U);
    batch->retained_bytes = retained;
    return batch;
  } catch (const std::bad_alloc &) {
    throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                           "XREAD validation state allocation failed");
  } catch (const std::length_error &) {
    throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                           "XREAD validation state allocation failed");
  }
}

void RedisXReadState::require_record_capacity(
    const BufferedBatch &batch, const std::size_t stream_name_bytes,
    const std::size_t stream_id_bytes, const std::size_t field_name_bytes,
    const std::size_t value_bytes) const {
  if (batch.records.size() >= limits_.max_records_per_read) {
    throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                           "XREAD record-count limit exceeded");
  }
  if (stream_name_bytes > kMaxStreamNameBytes) {
    throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                           "XREAD stream name exceeds the byte limit");
  }
  if (stream_id_bytes > kMaxRenderedStreamIdBytes) {
    throw RedisReaderError(RedisReaderErrorCode::InvalidStreamId,
                           "Redis stream ID is oversized");
  }
  if (field_name_bytes > kMaxFieldNameBytes) {
    throw RedisReaderError(RedisReaderErrorCode::FieldContractViolation,
                           "Redis entry field name is oversized");
  }

  auto entry_bytes = checked_sum(stream_id_bytes, field_name_bytes);
  entry_bytes = checked_sum(entry_bytes, value_bytes);
  if (entry_bytes > limits_.max_entry_bytes) {
    throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                           "XREAD entry byte limit exceeded");
  }

  const auto retained = retained_record_bytes(
      stream_name_bytes, stream_id_bytes, field_name_bytes, value_bytes);
  const auto currently_retained =
      checked_sum(batch.retained_bytes, batch.transient_bytes);
  if (currently_retained > limits_.max_batch_bytes ||
      retained > limits_.max_batch_bytes - currently_retained) {
    throw RedisReaderError(
        RedisReaderErrorCode::ResourceExhausted,
        "XREAD retained batch storage exceeds the byte limit");
  }
}

void RedisXReadState::append_record(BufferedBatch &batch,
                                    const std::size_t stream_index,
                                    const std::string_view stream_name,
                                    std::string stream_id,
                                    std::string field_name,
                                    std::string value) const {
  if (stream_index >= streams_.size() ||
      stream_index >= batch.last_ids.size()) {
    throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                           "XREAD record references an invalid stream");
  }
  require_record_capacity(batch, stream_name.size(), stream_id.size(),
                          field_name.size(), value.size());
  const auto &required_field = streams_[stream_index].required_field_name;
  if (required_field.has_value() && field_name != *required_field) {
    throw RedisReaderError(
        RedisReaderErrorCode::FieldContractViolation,
        "Redis entry field name violates its stream contract");
  }

  const StreamId parsed_id = parse_stream_id(stream_id);
  if (parsed_id == batch.last_ids[stream_index]) {
    throw RedisReaderError(RedisReaderErrorCode::DuplicateEntry,
                           "XREAD returned a duplicate stream entry ID");
  }
  if (parsed_id < batch.last_ids[stream_index]) {
    throw RedisReaderError(RedisReaderErrorCode::ReorderedEntry,
                           "XREAD returned a regressing stream entry ID");
  }

  const auto retained = retained_record_bytes(
      stream_name.size(), stream_id.size(), field_name.size(), value.size());
  try {
    batch.records.push_back(ParsedRecord{
        RedisStreamRecord{std::string(stream_name), std::move(stream_id),
                          std::move(field_name), std::move(value)},
        parsed_id, stream_index});
  } catch (const std::bad_alloc &) {
    throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                           "XREAD retained record allocation failed");
  } catch (const std::length_error &) {
    throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                           "XREAD retained record allocation failed");
  }
  batch.last_ids[stream_index] = parsed_id;
  batch.retained_bytes = checked_sum(batch.retained_bytes, retained);
}

std::size_t RedisXReadState::deliver(BufferedBatch &batch,
                                     const RedisRecordCallback &callback) {
  std::size_t delivered = 0U;
  for (ParsedRecord &parsed_record : batch.records) {
    bool accepted = false;
    try {
      accepted = callback(parsed_record.record);
    } catch (...) {
      std::throw_with_nested(RedisReaderError(
          RedisReaderErrorCode::CallbackFailed,
          "Redis record callback threw before cursor advancement"));
    }
    if (!accepted) {
      throw RedisReaderError(
          RedisReaderErrorCode::CallbackRejected,
          "Redis record callback rejected evidence before cursor advancement");
    }
    CursorState &cursor_state = cursors_[parsed_record.stream_index];
    // The accepted record owns the exact next cursor string. Swapping commits
    // it without allocation after downstream evidence has been accepted.
    cursor_state.rendered.swap(parsed_record.record.stream_id);
    cursor_state.parsed = parsed_record.parsed_id;
    ++delivered;
  }
  return delivered;
}

std::size_t RedisXReadState::consume(const redisReply *reply,
                                     const RedisRecordCallback &callback) {
  if (!callback) {
    throw RedisReaderError(RedisReaderErrorCode::InvalidConfiguration,
                           "Redis record callback must be set");
  }
  if (reply == nullptr) {
    throw RedisReaderError(RedisReaderErrorCode::ConnectionFailed,
                           "hiredis returned no XREAD reply");
  }
  if (reply->type == REDIS_REPLY_NIL) {
    return 0U;
  }
  if (reply->type == REDIS_REPLY_ERROR) {
    if ((reply->str == nullptr && reply->len != 0U) ||
        reply->len > std::min(kMaxServerErrorBytes, limits_.max_batch_bytes)) {
      throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                             "Redis error reply exceeds its byte limit");
    }
    std::string error_text;
    try {
      if (reply->str != nullptr) {
        error_text.assign(reply->str, reply->len);
      }
    } catch (const std::bad_alloc &) {
      throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                             "Redis error reply allocation failed");
    } catch (const std::length_error &) {
      throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                             "Redis error reply allocation failed");
    }
    throw RedisReaderError(RedisReaderErrorCode::ServerError,
                           "Redis rejected XREAD: " + error_text);
  }
  require_array(reply, "XREAD root");
  if (reply->elements == 0U) {
    throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                           "XREAD returned an empty non-nil array");
  }
  if (reply->elements > streams_.size()) {
    throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                           "XREAD returned more streams than requested");
  }

  auto batch = new_batch();
  for (std::size_t stream_position = 0; stream_position < reply->elements;
       ++stream_position) {
    const redisReply *const stream_reply = reply->element[stream_position];
    require_array(stream_reply, "XREAD stream result");
    if (stream_reply->elements != 2U) {
      throw RedisReaderError(
          RedisReaderErrorCode::MalformedReply,
          "XREAD stream result must have exactly two elements");
    }

    const redisReply *const stream_name_reply = stream_reply->element[0];
    const std::size_t stream_name_bytes = binary_size(stream_name_reply);
    if (stream_name_bytes > kMaxStreamNameBytes) {
      throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                             "XREAD stream name exceeds the byte limit");
    }
    const std::string_view stream_name =
        stream_name_bytes == 0U
            ? std::string_view{}
            : std::string_view(stream_name_reply->str, stream_name_bytes);
    const auto stream_index = find_stream_index(streams_, stream_name);
    if (!stream_index.has_value()) {
      throw RedisReaderError(RedisReaderErrorCode::UnknownStream,
                             "XREAD returned an unconfigured " +
                                 describe_stream(stream_name));
    }
    if (batch->returned_streams[*stream_index] != 0U) {
      throw RedisReaderError(RedisReaderErrorCode::DuplicateStream,
                             "XREAD returned one stream more than once");
    }
    batch->returned_streams[*stream_index] = 1U;

    const redisReply *const entries_reply = stream_reply->element[1];
    require_array(entries_reply, "XREAD stream entries");
    if (entries_reply->elements == 0U) {
      throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                             "XREAD returned an empty stream entry array");
    }
    if (entries_reply->elements >
        limits_.max_records_per_read - batch->records.size()) {
      throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                             "XREAD record-count limit exceeded");
    }

    for (std::size_t entry_position = 0;
         entry_position < entries_reply->elements; ++entry_position) {
      const redisReply *const entry_reply =
          entries_reply->element[entry_position];
      require_array(entry_reply, "XREAD entry");
      if (entry_reply->elements != 2U) {
        throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                               "XREAD entry must have exactly two elements");
      }

      const redisReply *const fields_reply = entry_reply->element[1];
      require_array(fields_reply, "XREAD entry fields");
      if (fields_reply->elements != 2U) {
        throw RedisReaderError(
            RedisReaderErrorCode::FieldContractViolation,
            "audited Redis entry must contain exactly one field/value pair");
      }

      const auto stream_id_bytes = binary_size(entry_reply->element[0]);
      const auto field_bytes = binary_size(fields_reply->element[0]);
      const auto value_bytes = binary_size(fields_reply->element[1]);
      require_record_capacity(*batch, stream_name.size(), stream_id_bytes,
                              field_bytes, value_bytes);
      append_record(*batch, *stream_index, stream_name,
                    binary_string(entry_reply->element[0]),
                    binary_string(fields_reply->element[0]),
                    binary_string(fields_reply->element[1]));
    }
  }

  return deliver(*batch, callback);
}

BlockingRedisReader::BlockingRedisReader(
    redisContext *context, std::vector<RedisStreamSpec> streams,
    std::size_t count, std::chrono::milliseconds block_timeout,
    RedisReaderLimits limits)
    : context_(context), state_(std::move(streams), limits), limits_(limits),
      count_(count), block_timeout_(block_timeout) {
  if (context_ == nullptr) {
    throw RedisReaderError(RedisReaderErrorCode::InvalidConfiguration,
                           "hiredis context must not be null");
  }
  if (count_ == 0U) {
    throw RedisReaderError(RedisReaderErrorCode::InvalidConfiguration,
                           "XREAD COUNT must be positive");
  }
  if (count_ > limits.max_records_per_read / state_.streams().size()) {
    throw RedisReaderError(
        RedisReaderErrorCode::InvalidConfiguration,
        "XREAD COUNT and stream count exceed the record-count limit");
  }
  if (block_timeout_.count() < 0) {
    throw RedisReaderError(RedisReaderErrorCode::InvalidConfiguration,
                           "XREAD BLOCK timeout must not be negative");
  }
  if (state_.streams().size() > static_cast<std::size_t>((INT_MAX - 7) / 2)) {
    throw RedisReaderError(RedisReaderErrorCode::InvalidConfiguration,
                           "too many Redis streams for hiredis argv");
  }
}

std::string_view
BlockingRedisReader::cursor(std::string_view stream_name) const {
  return state_.cursor(stream_name);
}

std::vector<std::string> BlockingRedisReader::xread_arguments() const {
  std::vector<std::string> arguments{"XREAD",
                                     "BLOCK",
                                     std::to_string(block_timeout_.count()),
                                     "COUNT",
                                     std::to_string(count_),
                                     "STREAMS"};
  std::vector<std::string> streams_and_cursors =
      state_.xread_streams_and_cursors();
  arguments.reserve(arguments.size() + streams_and_cursors.size());
  for (std::string &argument : streams_and_cursors) {
    arguments.push_back(std::move(argument));
  }
  return arguments;
}

std::size_t
BlockingRedisReader::read_once(const RedisRecordCallback &callback) {
  if (!callback) {
    throw RedisReaderError(RedisReaderErrorCode::InvalidConfiguration,
                           "Redis record callback must be set");
  }
  if (transport_poisoned_) {
    throw RedisReaderError(
        RedisReaderErrorCode::ConnectionFailed,
        "Redis transport cannot be reused after an incomplete reply");
  }
  if (context_->err != 0) {
    throw RedisReaderError(RedisReaderErrorCode::ConnectionFailed,
                           std::string("hiredis context is failed: ") +
                               context_->errstr);
  }
  if ((context_->flags & REDIS_BLOCK) == 0) {
    throw RedisReaderError(RedisReaderErrorCode::InvalidConfiguration,
                           "BlockingRedisReader requires a blocking context");
  }
  if (context_->privctx != nullptr) {
    throw RedisReaderError(
        RedisReaderErrorCode::InvalidConfiguration,
        "BlockingRedisReader supports only the pinned plaintext transport");
  }
  if (context_->reader != nullptr &&
      (context_->reader->pos != context_->reader->len ||
       context_->reader->ridx != -1 || context_->reader->reply != nullptr)) {
    throw RedisReaderError(
        RedisReaderErrorCode::InvalidConfiguration,
        "hiredis context contains a reply buffered by another consumer");
  }
  if (context_->obuf != nullptr && sdslen(context_->obuf) != 0U) {
    throw RedisReaderError(
        RedisReaderErrorCode::InvalidConfiguration,
        "hiredis context contains a command buffered by another producer");
  }
  try {
    require_clean_transport_before_command(context_);
  } catch (...) {
    transport_poisoned_ = true;
    throw;
  }

  std::vector<std::string> arguments;
  std::vector<const char *> argv;
  std::vector<std::size_t> argument_lengths;
  try {
    arguments = xread_arguments();
    argv.reserve(arguments.size());
    argument_lengths.reserve(arguments.size());
    for (const std::string &argument : arguments) {
      argv.push_back(argument.data());
      argument_lengths.push_back(argument.size());
    }
  } catch (const std::bad_alloc &) {
    throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                           "XREAD command argument allocation failed");
  } catch (const std::length_error &) {
    throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                           "XREAD command argument allocation failed");
  }

  if (redisAppendCommandArgv(context_, static_cast<int>(argv.size()),
                             argv.data(),
                             argument_lengths.data()) != REDIS_OK) {
    const std::string detail = context_->err == 0
                                   ? std::string("no hiredis error detail")
                                   : std::string(context_->errstr);
    const RedisReaderErrorCode code =
        context_->err == REDIS_ERR_OOM ? RedisReaderErrorCode::ResourceExhausted
                                       : RedisReaderErrorCode::ConnectionFailed;
    throw RedisReaderError(code, "XREAD command formatting failed: " + detail);
  }

  int write_complete = 0;
  while (write_complete == 0) {
    if (redisBufferWrite(context_, &write_complete) != REDIS_OK) {
      transport_poisoned_ = true;
      const std::string detail = context_->err == 0
                                     ? std::string("no hiredis error detail")
                                     : std::string(context_->errstr);
      const RedisReaderErrorCode code =
          context_->err == REDIS_ERR_OOM
              ? RedisReaderErrorCode::ResourceExhausted
              : RedisReaderErrorCode::ConnectionFailed;
      throw RedisReaderError(code, "XREAD command write failed: " + detail);
    }
  }

  BoundedRespReader response(context_);
  bool response_complete = false;
  try {
    const char root_type = response.read_type();
    if (root_type == '-') {
      const std::string error_text = response.read_line(
          std::min(kMaxServerErrorBytes, limits_.max_batch_bytes),
          "Redis error reply");
      if (response.has_buffered_bytes()) {
        throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                               "Redis error reply has trailing bytes");
      }
      require_no_queued_transport_bytes(context_);
      response_complete = true;
      throw RedisReaderError(RedisReaderErrorCode::ServerError,
                             "Redis rejected XREAD: " + error_text);
    }
    if (root_type != '*') {
      throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                             "XREAD root is not a RESP array");
    }

    const std::string root_length =
        response.read_line(kMaxRespNumberBytes, "XREAD root");
    if (root_length == "-1") {
      if (response.has_buffered_bytes()) {
        throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                               "nil XREAD reply has trailing bytes");
      }
      require_no_queued_transport_bytes(context_);
      response_complete = true;
      return 0U;
    }
    const auto stream_count = parse_resp_size(root_length);
    if (!stream_count.has_value()) {
      throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                             "XREAD root has an invalid RESP array length");
    }
    if (*stream_count == 0U) {
      throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                             "XREAD returned an empty non-nil array");
    }
    if (*stream_count > state_.streams().size()) {
      throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                             "XREAD returned more streams than requested");
    }

    auto batch = state_.new_batch();

    for (std::size_t stream_position = 0; stream_position < *stream_count;
         ++stream_position) {
      if (response.read_array_length("XREAD stream result") != 2U) {
        throw RedisReaderError(
            RedisReaderErrorCode::MalformedReply,
            "XREAD stream result must have exactly two elements");
      }
      const std::size_t stream_name_bytes =
          response.read_bulk_length("XREAD stream name");
      if (stream_name_bytes > kMaxStreamNameBytes) {
        throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                               "XREAD stream name exceeds its byte limit");
      }
      const std::size_t stream_name_storage = checked_sum(
          sizeof(std::string), retained_string_bytes(stream_name_bytes));
      if (batch->retained_bytes > limits_.max_batch_bytes ||
          stream_name_storage >
              limits_.max_batch_bytes - batch->retained_bytes) {
        throw RedisReaderError(
            RedisReaderErrorCode::ResourceExhausted,
            "XREAD stream-name storage exceeds the retained batch byte limit");
      }
      batch->transient_bytes = stream_name_storage;
      const std::string stream_name =
          response.read_bulk_body(stream_name_bytes, "XREAD stream name");
      const auto stream_index =
          find_stream_index(state_.streams(), stream_name);
      if (!stream_index.has_value()) {
        throw RedisReaderError(RedisReaderErrorCode::UnknownStream,
                               "XREAD returned an unconfigured " +
                                   describe_stream(stream_name));
      }
      if (batch->returned_streams[*stream_index] != 0U) {
        throw RedisReaderError(RedisReaderErrorCode::DuplicateStream,
                               "XREAD returned one stream more than once");
      }
      batch->returned_streams[*stream_index] = 1U;

      const std::size_t entry_count =
          response.read_array_length("XREAD stream entries");
      if (entry_count == 0U) {
        throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                               "XREAD returned an empty stream entry array");
      }
      if (entry_count > count_) {
        throw RedisReaderError(
            RedisReaderErrorCode::MalformedReply,
            "XREAD returned more stream entries than requested by COUNT");
      }
      if (entry_count > limits_.max_records_per_read - batch->records.size()) {
        throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                               "XREAD record-count limit exceeded");
      }

      for (std::size_t entry_position = 0; entry_position < entry_count;
           ++entry_position) {
        if (response.read_array_length("XREAD entry") != 2U) {
          throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                                 "XREAD entry must have exactly two elements");
        }
        const std::size_t stream_id_bytes =
            response.read_bulk_length("Redis stream ID");
        if (stream_id_bytes > kMaxRenderedStreamIdBytes) {
          throw RedisReaderError(RedisReaderErrorCode::InvalidStreamId,
                                 "Redis stream ID exceeds its byte limit");
        }
        state_.require_record_capacity(*batch, stream_name.size(),
                                       stream_id_bytes, 0U, 0U);
        std::string stream_id =
            response.read_bulk_body(stream_id_bytes, "Redis stream ID");
        if (response.read_array_length("XREAD entry fields") != 2U) {
          throw RedisReaderError(
              RedisReaderErrorCode::FieldContractViolation,
              "audited Redis entry must contain exactly one field/value pair");
        }
        const std::size_t field_name_bytes =
            response.read_bulk_length("Redis entry field name");
        if (field_name_bytes > kMaxFieldNameBytes) {
          throw RedisReaderError(
              RedisReaderErrorCode::FieldContractViolation,
              "Redis entry field name exceeds its byte limit");
        }
        state_.require_record_capacity(*batch, stream_name.size(),
                                       stream_id_bytes, field_name_bytes, 0U);
        std::string field_name =
            response.read_bulk_body(field_name_bytes, "Redis entry field name");
        const std::size_t value_bytes =
            response.read_bulk_length("Redis entry value");
        state_.require_record_capacity(*batch, stream_name.size(),
                                       stream_id.size(), field_name.size(),
                                       value_bytes);
        std::string value =
            response.read_bulk_body(value_bytes, "Redis entry value");
        state_.append_record(*batch, *stream_index, stream_name,
                             std::move(stream_id), std::move(field_name),
                             std::move(value));
      }
      batch->transient_bytes = 0U;
    }

    if (response.has_buffered_bytes()) {
      throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                             "XREAD reply has trailing bytes");
    }
    require_no_queued_transport_bytes(context_);
    response_complete = true;
    return state_.deliver(*batch, callback);
  } catch (const std::bad_alloc &) {
    if (!response_complete) {
      transport_poisoned_ = true;
    }
    throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                           "XREAD response allocation failed");
  } catch (const std::length_error &) {
    if (!response_complete) {
      transport_poisoned_ = true;
    }
    throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                           "XREAD response allocation failed");
  } catch (...) {
    if (!response_complete) {
      transport_poisoned_ = true;
    }
    throw;
  }
}

} // namespace blackbox
