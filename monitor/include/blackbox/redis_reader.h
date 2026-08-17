#pragma once

#include <hiredis/hiredis.h>

#include <chrono>
#include <cstddef>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace blackbox {

enum class RedisReaderErrorCode {
  InvalidConfiguration,
  ConnectionFailed,
  ServerError,
  MalformedReply,
  UnknownStream,
  DuplicateStream,
  InvalidStreamId,
  DuplicateEntry,
  ReorderedEntry,
  FieldContractViolation,
  CallbackRejected,
  CallbackFailed,
  ResourceExhausted,
};

[[nodiscard]] std::string_view
redis_reader_error_name(RedisReaderErrorCode code) noexcept;

class RedisReaderError : public std::runtime_error {
public:
  RedisReaderError(RedisReaderErrorCode code, std::string message);

  [[nodiscard]] RedisReaderErrorCode code() const noexcept;

private:
  RedisReaderErrorCode code_;
};

// Every audited stream currently contains exactly one field per entry. CDC
// uses its Debezium key as the field name, while client/status streams use the
// literal field name "payload". Leaving required_field_name empty permits any
// single binary field name without interpreting it.
struct RedisStreamSpec {
  std::string stream_name;
  std::string initial_cursor;
  std::optional<std::string> required_field_name;
};

struct RedisStreamRecord {
  std::string stream_name;
  std::string stream_id;
  std::string field_name;
  std::string value;
};

struct RedisReaderLimits {
  std::size_t max_records_per_read = 100000;
  std::size_t max_entry_bytes = 64U * 1024U * 1024U;
  std::size_t max_batch_bytes = 256U * 1024U * 1024U;
};

using RedisRecordCallback = std::function<bool(const RedisStreamRecord &)>;

// Owns the explicit cursor for each configured stream and validates RESP2
// XREAD replies. It has no Redis connection and is intentionally usable by
// fixture/unit tests. Reply validation is complete before any callback runs.
class RedisXReadState {
public:
  explicit RedisXReadState(std::vector<RedisStreamSpec> streams,
                           RedisReaderLimits limits = {});
  ~RedisXReadState();
  RedisXReadState(const RedisXReadState &) = delete;
  RedisXReadState &operator=(const RedisXReadState &) = delete;
  RedisXReadState(RedisXReadState &&) = delete;
  RedisXReadState &operator=(RedisXReadState &&) = delete;

  [[nodiscard]] const std::vector<RedisStreamSpec> &streams() const noexcept;
  [[nodiscard]] std::string_view cursor(std::string_view stream_name) const;
  [[nodiscard]] std::vector<std::string> xread_streams_and_cursors() const;

  // A nil reply represents a finite BLOCK timeout and returns zero. For a
  // non-nil reply, callbacks run in RESP traversal order. That traversal order
  // carries no ordering meaning between different streams.
  std::size_t consume(const redisReply *reply,
                      const RedisRecordCallback &callback);

private:
  struct CursorState;

  std::vector<RedisStreamSpec> streams_;
  std::vector<CursorState> cursors_;
  RedisReaderLimits limits_;
};

// Executes one binary-safe `XREAD BLOCK ... COUNT ... STREAMS ...` at a time.
// The redisContext is borrowed and must outlive this object. This class does
// not reconnect: restart/recovery policy belongs to the run lifecycle.
class BlockingRedisReader {
public:
  BlockingRedisReader(redisContext *context,
                      std::vector<RedisStreamSpec> streams, std::size_t count,
                      std::chrono::milliseconds block_timeout,
                      RedisReaderLimits limits = {});
  BlockingRedisReader(const BlockingRedisReader &) = delete;
  BlockingRedisReader &operator=(const BlockingRedisReader &) = delete;
  BlockingRedisReader(BlockingRedisReader &&) = delete;
  BlockingRedisReader &operator=(BlockingRedisReader &&) = delete;

  [[nodiscard]] std::string_view cursor(std::string_view stream_name) const;

  // Exposed as an immutable diagnostic/test seam. All arguments, including
  // stream names and concrete cursors, are passed to hiredis with explicit
  // lengths by read_once().
  [[nodiscard]] std::vector<std::string> xread_arguments() const;

  std::size_t read_once(const RedisRecordCallback &callback);

private:
  redisContext *context_;
  RedisXReadState state_;
  std::size_t count_;
  std::chrono::milliseconds block_timeout_;
};

} // namespace blackbox
