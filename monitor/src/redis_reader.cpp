#include "blackbox/redis_reader.h"

#include <charconv>
#include <climits>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace blackbox {
namespace {

constexpr std::size_t kMaxStreamNameBytes = 1024U;
constexpr std::size_t kMaxFieldNameBytes = 1024U;
constexpr std::size_t kMaxRenderedStreamIdBytes = 41U;

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
  return std::string(reply->str, reply->len);
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

using ReplyPtr = std::unique_ptr<redisReply, decltype(&freeReplyObject)>;

} // namespace

struct RedisXReadState::CursorState {
  std::string rendered;
  StreamId parsed;
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

  std::unordered_set<std::string> names;
  names.reserve(streams_.size());
  cursors_.reserve(streams_.size());
  for (const RedisStreamSpec &stream : streams_) {
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
    if (!names.emplace(stream.stream_name).second) {
      throw RedisReaderError(
          RedisReaderErrorCode::InvalidConfiguration,
          "Redis stream configuration contains a duplicate name");
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
    const std::string error_text = reply->str == nullptr
                                       ? std::string{}
                                       : std::string(reply->str, reply->len);
    throw RedisReaderError(RedisReaderErrorCode::ServerError,
                           "Redis rejected XREAD: " + error_text);
  }
  require_array(reply, "XREAD root");
  if (reply->elements == 0U) {
    throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                           "XREAD returned an empty non-nil array");
  }

  std::unordered_map<std::string, std::size_t> configured_streams;
  configured_streams.reserve(streams_.size());
  for (std::size_t index = 0; index < streams_.size(); ++index) {
    configured_streams.emplace(streams_[index].stream_name, index);
  }

  std::unordered_set<std::string> returned_streams;
  returned_streams.reserve(reply->elements);
  std::vector<StreamId> last_ids;
  last_ids.reserve(cursors_.size());
  for (const CursorState &cursor_state : cursors_) {
    last_ids.push_back(cursor_state.parsed);
  }

  std::vector<ParsedRecord> parsed_records;
  std::size_t parsed_batch_bytes = 0U;
  for (std::size_t stream_position = 0; stream_position < reply->elements;
       ++stream_position) {
    const redisReply *const stream_reply = reply->element[stream_position];
    require_array(stream_reply, "XREAD stream result");
    if (stream_reply->elements != 2U) {
      throw RedisReaderError(
          RedisReaderErrorCode::MalformedReply,
          "XREAD stream result must have exactly two elements");
    }

    if (binary_size(stream_reply->element[0]) > kMaxStreamNameBytes) {
      throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                             "XREAD stream name exceeds the byte limit");
    }
    const std::string stream_name = binary_string(stream_reply->element[0]);
    const auto configured = configured_streams.find(stream_name);
    if (configured == configured_streams.end()) {
      throw RedisReaderError(RedisReaderErrorCode::UnknownStream,
                             "XREAD returned an unconfigured " +
                                 describe_stream(stream_name));
    }
    if (!returned_streams.emplace(stream_name).second) {
      throw RedisReaderError(RedisReaderErrorCode::DuplicateStream,
                             "XREAD returned one stream more than once");
    }

    const std::size_t stream_index = configured->second;
    const redisReply *const entries_reply = stream_reply->element[1];
    require_array(entries_reply, "XREAD stream entries");
    if (entries_reply->elements == 0U) {
      throw RedisReaderError(RedisReaderErrorCode::MalformedReply,
                             "XREAD returned an empty stream entry array");
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

      if (binary_size(entry_reply->element[0]) > kMaxRenderedStreamIdBytes) {
        throw RedisReaderError(RedisReaderErrorCode::InvalidStreamId,
                               "Redis stream ID is oversized");
      }
      const std::string stream_id = binary_string(entry_reply->element[0]);
      const StreamId parsed_id = parse_stream_id(stream_id);
      if (parsed_id == last_ids[stream_index]) {
        throw RedisReaderError(RedisReaderErrorCode::DuplicateEntry,
                               "XREAD returned a duplicate stream entry ID");
      }
      if (parsed_id < last_ids[stream_index]) {
        throw RedisReaderError(RedisReaderErrorCode::ReorderedEntry,
                               "XREAD returned a regressing stream entry ID");
      }
      last_ids[stream_index] = parsed_id;

      const redisReply *const fields_reply = entry_reply->element[1];
      require_array(fields_reply, "XREAD entry fields");
      if (fields_reply->elements != 2U) {
        throw RedisReaderError(
            RedisReaderErrorCode::FieldContractViolation,
            "audited Redis entry must contain exactly one field/value pair");
      }
      if (parsed_records.size() >= limits_.max_records_per_read) {
        throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                               "XREAD record-count limit exceeded");
      }
      const auto field_bytes = binary_size(fields_reply->element[0]);
      const auto value_bytes = binary_size(fields_reply->element[1]);
      if (field_bytes > kMaxFieldNameBytes) {
        throw RedisReaderError(RedisReaderErrorCode::FieldContractViolation,
                               "Redis entry field name is oversized");
      }
      auto entry_bytes = checked_sum(stream_id.size(), field_bytes);
      entry_bytes = checked_sum(entry_bytes, value_bytes);
      if (entry_bytes > limits_.max_entry_bytes) {
        throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                               "XREAD entry byte limit exceeded");
      }
      parsed_batch_bytes = checked_sum(parsed_batch_bytes, entry_bytes);
      if (parsed_batch_bytes > limits_.max_batch_bytes) {
        throw RedisReaderError(RedisReaderErrorCode::ResourceExhausted,
                               "XREAD batch byte limit exceeded");
      }
      std::string field_name = binary_string(fields_reply->element[0]);
      std::string value = binary_string(fields_reply->element[1]);
      const auto &required_field = streams_[stream_index].required_field_name;
      if (required_field.has_value() && field_name != *required_field) {
        throw RedisReaderError(
            RedisReaderErrorCode::FieldContractViolation,
            "Redis entry field name violates its stream contract");
      }

      parsed_records.push_back(ParsedRecord{
          RedisStreamRecord{stream_name, stream_id, std::move(field_name),
                            std::move(value)},
          parsed_id, stream_index});
    }
  }

  std::size_t delivered = 0U;
  for (const ParsedRecord &parsed_record : parsed_records) {
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
    cursor_state.rendered = parsed_record.record.stream_id;
    cursor_state.parsed = parsed_record.parsed_id;
    ++delivered;
  }
  return delivered;
}

BlockingRedisReader::BlockingRedisReader(
    redisContext *context, std::vector<RedisStreamSpec> streams,
    std::size_t count, std::chrono::milliseconds block_timeout,
    RedisReaderLimits limits)
    : context_(context), state_(std::move(streams), limits), count_(count),
      block_timeout_(block_timeout) {
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
  if (context_->err != 0) {
    throw RedisReaderError(RedisReaderErrorCode::ConnectionFailed,
                           std::string("hiredis context is failed: ") +
                               context_->errstr);
  }

  const std::vector<std::string> arguments = xread_arguments();
  std::vector<const char *> argv;
  std::vector<std::size_t> argument_lengths;
  argv.reserve(arguments.size());
  argument_lengths.reserve(arguments.size());
  for (const std::string &argument : arguments) {
    argv.push_back(argument.data());
    argument_lengths.push_back(argument.size());
  }

  ReplyPtr reply(static_cast<redisReply *>(
                     redisCommandArgv(context_, static_cast<int>(argv.size()),
                                      argv.data(), argument_lengths.data())),
                 &freeReplyObject);
  if (!reply) {
    const std::string detail = context_->err == 0
                                   ? std::string("no hiredis error detail")
                                   : std::string(context_->errstr);
    throw RedisReaderError(RedisReaderErrorCode::ConnectionFailed,
                           "XREAD failed: " + detail);
  }
  return state_.consume(reply.get(), callback);
}

} // namespace blackbox
