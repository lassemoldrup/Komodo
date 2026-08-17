#include "blackbox/redis_reader.h"
#include "test_support.h"

#include <hiredis/hiredis.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <sys/socket.h>
#include <unistd.h>

namespace {

using blackbox::BlockingRedisReader;
using blackbox::RedisReaderError;
using blackbox::RedisReaderErrorCode;
using blackbox::RedisReaderLimits;
using blackbox::RedisStreamRecord;
using blackbox::RedisStreamSpec;
using blackbox::RedisXReadState;

struct RedisContextDeleter {
  void operator()(redisContext *context) const noexcept {
    if (context != nullptr) {
      redisFree(context);
    }
  }
};

using RedisContextPtr = std::unique_ptr<redisContext, RedisContextDeleter>;

[[nodiscard]] char readSocketByte(const int descriptor) {
  char result{};
  for (;;) {
    const auto count = ::read(descriptor, &result, 1U);
    if (count == 1) {
      return result;
    }
    if (count < 0 && errno == EINTR) {
      continue;
    }
    throw std::runtime_error("scripted Redis peer read failed");
  }
}

[[nodiscard]] std::string readSocketLine(const int descriptor,
                                         std::string &raw) {
  std::string line;
  for (;;) {
    const char character = readSocketByte(descriptor);
    raw.push_back(character);
    if (character == '\r') {
      const char newline = readSocketByte(descriptor);
      raw.push_back(newline);
      if (newline != '\n') {
        throw std::runtime_error("scripted Redis peer saw malformed CRLF");
      }
      return line;
    }
    line.push_back(character);
  }
}

[[nodiscard]] std::size_t parseSocketSize(const std::string_view rendered) {
  if (rendered.empty()) {
    throw std::runtime_error("scripted Redis peer saw an empty size");
  }
  std::size_t result = 0U;
  for (const char character : rendered) {
    if (character < '0' || character > '9') {
      throw std::runtime_error("scripted Redis peer saw a malformed size");
    }
    result = result * 10U + static_cast<std::size_t>(character - '0');
  }
  return result;
}

void readSocketBytes(const int descriptor, const std::size_t bytes,
                     std::string &raw) {
  for (std::size_t index = 0; index < bytes; ++index) {
    raw.push_back(readSocketByte(descriptor));
  }
}

[[nodiscard]] std::string readRespCommand(const int descriptor) {
  std::string raw;
  if (readSocketByte(descriptor) != '*') {
    throw std::runtime_error("scripted Redis peer expected a command array");
  }
  raw.push_back('*');
  const std::size_t argument_count =
      parseSocketSize(readSocketLine(descriptor, raw));
  for (std::size_t index = 0; index < argument_count; ++index) {
    if (readSocketByte(descriptor) != '$') {
      throw std::runtime_error("scripted Redis peer expected a bulk argument");
    }
    raw.push_back('$');
    const std::size_t argument_bytes =
        parseSocketSize(readSocketLine(descriptor, raw));
    readSocketBytes(descriptor, argument_bytes + 2U, raw);
    if (raw.size() < 2U || raw[raw.size() - 2U] != '\r' || raw.back() != '\n') {
      throw std::runtime_error("scripted Redis peer saw a malformed argument");
    }
  }
  return raw;
}

void writeSocketBytes(const int descriptor, const std::string_view bytes) {
  std::size_t written = 0U;
  while (written != bytes.size()) {
    const auto result =
        ::write(descriptor, bytes.data() + written, bytes.size() - written);
    if (result < 0 && errno == EINTR) {
      continue;
    }
    if (result <= 0) {
      throw std::runtime_error("scripted Redis peer write failed");
    }
    written += static_cast<std::size_t>(result);
  }
}

class SocketReplyRedisConnection {
public:
  explicit SocketReplyRedisConnection(std::vector<std::string> replies,
                                      const bool close_after_reply = false)
      : replies_(std::move(replies)), close_after_reply_(close_after_reply) {
    int sockets[2] = {-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) != 0) {
      throw std::runtime_error("socketpair failed");
    }
    peer_fd_ = sockets[1];
    context_.reset(redisConnectFd(sockets[0]));
    if (!context_ || context_->err != 0) {
      ::close(peer_fd_);
      peer_fd_ = -1;
      if (!context_) {
        ::close(sockets[0]);
      }
      throw std::runtime_error("redisConnectFd failed");
    }

    try {
      peer_thread_ = std::thread([this] {
        try {
          for (const std::string &reply : replies_) {
            commands_.push_back(readRespCommand(peer_fd_));
            writeSocketBytes(peer_fd_, reply);
          }
          if (close_after_reply_ && ::shutdown(peer_fd_, SHUT_WR) != 0) {
            throw std::runtime_error("scripted Redis peer shutdown failed");
          }
        } catch (...) {
          peer_error_ = std::current_exception();
          (void)::shutdown(peer_fd_, SHUT_WR);
        }
      });
    } catch (...) {
      ::close(peer_fd_);
      peer_fd_ = -1;
      throw;
    }
  }

  ~SocketReplyRedisConnection() {
    if (peer_thread_.joinable()) {
      (void)::shutdown(peer_fd_, SHUT_RDWR);
      peer_thread_.join();
    }
    if (peer_fd_ >= 0) {
      ::close(peer_fd_);
    }
  }

  SocketReplyRedisConnection(const SocketReplyRedisConnection &) = delete;
  SocketReplyRedisConnection &
  operator=(const SocketReplyRedisConnection &) = delete;

  [[nodiscard]] redisContext *context() const noexcept {
    return context_.get();
  }

  void finish() {
    if (peer_thread_.joinable()) {
      peer_thread_.join();
    }
    if (peer_error_ != nullptr) {
      std::rethrow_exception(peer_error_);
    }
  }

  [[nodiscard]] const std::vector<std::string> &commands() const noexcept {
    return commands_;
  }

private:
  std::vector<std::string> replies_;
  std::vector<std::string> commands_;
  bool close_after_reply_{};
  std::exception_ptr peer_error_;
  int peer_fd_ = -1;
  RedisContextPtr context_;
  std::thread peer_thread_;
};

struct ScriptedTransport {
  std::vector<std::string> replies;
  std::vector<std::string> commands;
  std::size_t next_response{};
  std::size_t active_response{};
  std::size_t response_offset{};
  std::size_t recoverable_reads{};
  int peer_fd = -1;
  bool fail_read_oom{};
  bool close_peer_after_response{};
  bool response_active{};
};

void setScriptedError(redisContext *context, const int code) noexcept {
  context->err = code;
  context->errstr[0] = 'x';
  context->errstr[1] = '\0';
}

ssize_t scriptedRead(redisContext *context, char *destination,
                     const std::size_t capacity) noexcept {
  auto *const transport = static_cast<ScriptedTransport *>(context->privdata);
  if (transport == nullptr || !transport->response_active ||
      transport->active_response >= transport->replies.size()) {
    setScriptedError(context, REDIS_ERR_OTHER);
    return -1;
  }
  if (transport->recoverable_reads != 0U) {
    --transport->recoverable_reads;
    return 0;
  }
  if (transport->fail_read_oom) {
    transport->fail_read_oom = false;
    setScriptedError(context, REDIS_ERR_OOM);
    return -1;
  }
  const std::string &reply = transport->replies[transport->active_response];
  if (transport->response_offset >= reply.size()) {
    setScriptedError(context, REDIS_ERR_EOF);
    return -1;
  }
  const std::size_t count =
      std::min(capacity, reply.size() - transport->response_offset);
  std::copy_n(reply.data() + transport->response_offset, count, destination);
  transport->response_offset += count;
  if (transport->close_peer_after_response &&
      transport->response_offset == reply.size()) {
    (void)::shutdown(transport->peer_fd, SHUT_WR);
  }
  return static_cast<ssize_t>(count);
}

ssize_t scriptedWrite(redisContext *context) noexcept {
  auto *const transport = static_cast<ScriptedTransport *>(context->privdata);
  if (transport == nullptr ||
      transport->next_response >= transport->replies.size()) {
    setScriptedError(context, REDIS_ERR_OTHER);
    return -1;
  }
  const std::size_t command_bytes = sdslen(context->obuf);
  try {
    transport->commands.emplace_back(context->obuf, command_bytes);
  } catch (...) {
    setScriptedError(context, REDIS_ERR_OOM);
    return -1;
  }
  transport->active_response = transport->next_response++;
  transport->response_offset = 0U;
  transport->response_active = true;
  return static_cast<ssize_t>(command_bytes);
}

class ScriptedRedisConnection {
public:
  explicit ScriptedRedisConnection(std::vector<std::string> replies,
                                   const std::size_t recoverable_reads = 0U,
                                   const bool fail_read_oom = false,
                                   const bool close_peer_after_response = false)
      : transport_{} {
    transport_.replies = std::move(replies);
    transport_.recoverable_reads = recoverable_reads;
    transport_.fail_read_oom = fail_read_oom;
    transport_.close_peer_after_response = close_peer_after_response;
    int sockets[2] = {-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) != 0) {
      throw std::runtime_error("scripted socketpair failed");
    }
    peer_fd_ = sockets[1];
    transport_.peer_fd = peer_fd_;
    context_.reset(redisConnectFd(sockets[0]));
    if (!context_ || context_->err != 0) {
      ::close(peer_fd_);
      peer_fd_ = -1;
      if (!context_) {
        ::close(sockets[0]);
      }
      throw std::runtime_error("scripted redisConnectFd failed");
    }
    context_functions_ = *context_->funcs;
    context_functions_.read = &scriptedRead;
    context_functions_.write = &scriptedWrite;
    context_->funcs = &context_functions_;
    context_->privdata = &transport_;
  }

  ~ScriptedRedisConnection() {
    if (peer_fd_ >= 0) {
      ::close(peer_fd_);
    }
  }

  ScriptedRedisConnection(const ScriptedRedisConnection &) = delete;
  ScriptedRedisConnection &operator=(const ScriptedRedisConnection &) = delete;

  [[nodiscard]] redisContext *context() const noexcept {
    return context_.get();
  }
  [[nodiscard]] const std::vector<std::string> &commands() const noexcept {
    return transport_.commands;
  }
  void queuePeerBytes(const std::string_view bytes) const {
    writeSocketBytes(peer_fd_, bytes);
  }
  void closePeerWrite() const {
    if (::shutdown(peer_fd_, SHUT_WR) != 0) {
      throw std::runtime_error("scripted peer shutdown failed");
    }
  }

private:
  ScriptedTransport transport_;
  redisContextFuncs context_functions_{};
  int peer_fd_ = -1;
  RedisContextPtr context_;
};

[[nodiscard]] std::string respBulk(const std::string_view value) {
  return "$" + std::to_string(value.size()) + "\r\n" + std::string(value) +
         "\r\n";
}

[[nodiscard]] std::string respArray(std::vector<std::string> elements) {
  std::string result = "*" + std::to_string(elements.size()) + "\r\n";
  for (std::string &element : elements) {
    result += element;
  }
  return result;
}

[[nodiscard]] std::string respEntry(const std::string_view id,
                                    const std::string_view field,
                                    const std::string_view value) {
  return respArray(
      {respBulk(id), respArray({respBulk(field), respBulk(value)})});
}

[[nodiscard]] std::string respStream(const std::string_view name,
                                     std::vector<std::string> entries) {
  return respArray({respBulk(name), respArray(std::move(entries))});
}

struct ReplyNode;
using ReplyNodePtr = std::unique_ptr<ReplyNode>;

struct ReplyNode {
  redisReply reply{};
  std::string text;
  std::vector<ReplyNodePtr> children;
  std::vector<redisReply *> child_replies;
};

[[nodiscard]] ReplyNodePtr bulk(std::string value) {
  auto node = std::make_unique<ReplyNode>();
  node->text = std::move(value);
  node->reply.type = REDIS_REPLY_STRING;
  node->reply.str = node->text.data();
  node->reply.len = node->text.size();
  return node;
}

[[nodiscard]] ReplyNodePtr status(std::string value) {
  auto node = bulk(std::move(value));
  node->reply.type = REDIS_REPLY_STATUS;
  return node;
}

[[nodiscard]] ReplyNodePtr errorReply(std::string value) {
  auto node = bulk(std::move(value));
  node->reply.type = REDIS_REPLY_ERROR;
  return node;
}

[[nodiscard]] ReplyNodePtr nilReply() {
  auto node = std::make_unique<ReplyNode>();
  node->reply.type = REDIS_REPLY_NIL;
  return node;
}

template <typename... Children>
[[nodiscard]] ReplyNodePtr array(Children... children) {
  auto node = std::make_unique<ReplyNode>();
  node->reply.type = REDIS_REPLY_ARRAY;
  node->children.reserve(sizeof...(children));
  (node->children.push_back(std::move(children)), ...);
  node->child_replies.reserve(node->children.size());
  for (const auto &child : node->children) {
    node->child_replies.push_back(&child->reply);
  }
  node->reply.elements = node->child_replies.size();
  node->reply.element = node->child_replies.data();
  return node;
}

[[nodiscard]] ReplyNodePtr entry(std::string id, std::string field,
                                 std::string value) {
  return array(bulk(std::move(id)),
               array(bulk(std::move(field)), bulk(std::move(value))));
}

template <typename... Entries>
[[nodiscard]] ReplyNodePtr stream(std::string name, Entries... entries) {
  return array(bulk(std::move(name)), array(std::move(entries)...));
}

template <typename Operation>
[[nodiscard]] bool rejects(const RedisReaderErrorCode expected,
                           Operation &&operation) {
  try {
    std::forward<Operation>(operation)();
  } catch (const RedisReaderError &error) {
    return error.code() == expected;
  } catch (...) {
    return false;
  }
  return false;
}

[[nodiscard]] std::vector<RedisStreamSpec> twoStreams() {
  return {
      RedisStreamSpec{"cdc", "0-0", std::nullopt},
      RedisStreamSpec{"blackbox:client:run", "7-5", std::string("payload")},
  };
}

void checkConfigurationAndCommand() {
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::InvalidConfiguration,
                         [] { RedisXReadState state({}); }));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::InvalidConfiguration, [] {
    RedisXReadState state({RedisStreamSpec{"cdc", "$", std::nullopt}});
  }));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::InvalidConfiguration, [] {
    RedisXReadState state({RedisStreamSpec{"cdc", "0-0", std::nullopt}},
                          RedisReaderLimits{.max_records_per_read = 1U,
                                            .max_entry_bytes = 10U,
                                            .max_batch_bytes = 9U});
  }));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::InvalidConfiguration, [] {
    RedisXReadState state({RedisStreamSpec{"cdc", "0-0", std::nullopt},
                           RedisStreamSpec{"cdc", "1-0", std::nullopt}});
  }));

  redisContext context{};
  BlockingRedisReader reader(&context, twoStreams(), 17U,
                             std::chrono::milliseconds(250));
  const std::vector<std::string> expected = {
      "XREAD", "BLOCK",   "250", "COUNT",
      "17",    "STREAMS", "cdc", "blackbox:client:run",
      "0-0",   "7-5"};
  BLACKBOX_CHECK(reader.xread_arguments() == expected);
  BLACKBOX_CHECK(reader.cursor("cdc") == "0-0");

  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::InvalidConfiguration, [&] {
    BlockingRedisReader invalid(&context, twoStreams(), 0U,
                                std::chrono::milliseconds(1));
  }));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::InvalidConfiguration, [&] {
    BlockingRedisReader invalid(&context, twoStreams(), 1U,
                                std::chrono::milliseconds(-1));
  }));
}

void checkValidBinarySafeMultiStreamReply() {
  RedisXReadState state(twoStreams());
  const std::string client_value("client\0payload", 14);
  const std::string cdc_field("key\0json", 8);
  const std::string cdc_value("envelope\0json", 13);
  auto reply = array(
      stream("blackbox:client:run", entry("8-0", "payload", client_value)),
      stream("cdc", entry("1-0", cdc_field, cdc_value),
             entry("3-4", "second-key", "second-envelope")));

  std::vector<RedisStreamRecord> delivered;
  const std::size_t count =
      state.consume(&reply->reply, [&](const RedisStreamRecord &record) {
        delivered.push_back(record);
        return true;
      });

  BLACKBOX_CHECK(count == 3U);
  BLACKBOX_REQUIRE(delivered.size() == 3U);
  BLACKBOX_CHECK(delivered[0].stream_name == "blackbox:client:run");
  BLACKBOX_CHECK(delivered[0].value == client_value);
  BLACKBOX_CHECK(delivered[1].field_name == cdc_field);
  BLACKBOX_CHECK(delivered[1].value == cdc_value);
  BLACKBOX_CHECK(state.cursor("blackbox:client:run") == "8-0");
  BLACKBOX_CHECK(state.cursor("cdc") == "3-4");

  const std::vector<std::string> expected = {"cdc", "blackbox:client:run",
                                             "3-4", "8-0"};
  BLACKBOX_CHECK(state.xread_streams_and_cursors() == expected);
}

void checkWholeReplyValidationBeforeDelivery() {
  RedisXReadState state(twoStreams());
  auto reply = array(stream("cdc", entry("1-0", "key", "value")),
                     stream("unknown", entry("2-0", "key", "value")));
  std::size_t callbacks = 0U;
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::UnknownStream, [&] {
    (void)state.consume(&reply->reply, [&](const RedisStreamRecord &) {
      ++callbacks;
      return true;
    });
  }));
  BLACKBOX_CHECK(callbacks == 0U);
  BLACKBOX_CHECK(state.cursor("cdc") == "0-0");

  auto malformed_tail =
      array(stream("cdc", entry("1-0", "key", "value"),
                   array(bulk("2-0"), array(bulk("field-only")))));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::FieldContractViolation, [&] {
    (void)state.consume(&malformed_tail->reply, [&](const RedisStreamRecord &) {
      ++callbacks;
      return true;
    });
  }));
  BLACKBOX_CHECK(callbacks == 0U);
  BLACKBOX_CHECK(state.cursor("cdc") == "0-0");
}

void checkDuplicateAndReorderedEvidence() {
  RedisXReadState state(twoStreams());
  auto duplicate_stream = array(stream("cdc", entry("1-0", "key", "value")),
                                stream("cdc", entry("2-0", "key", "value")));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::DuplicateStream, [&] {
    (void)state.consume(&duplicate_stream->reply,
                        [](const RedisStreamRecord &) { return true; });
  }));

  auto duplicate_entry =
      array(stream("cdc", entry("1-0", "a", "a"), entry("1-0", "b", "b")));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::DuplicateEntry, [&] {
    (void)state.consume(&duplicate_entry->reply,
                        [](const RedisStreamRecord &) { return true; });
  }));

  auto reordered_entry =
      array(stream("cdc", entry("2-0", "a", "a"), entry("1-9", "b", "b")));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::ReorderedEntry, [&] {
    (void)state.consume(&reordered_entry->reply,
                        [](const RedisStreamRecord &) { return true; });
  }));

  auto accepted = array(stream("cdc", entry("5-0", "a", "a")));
  BLACKBOX_CHECK(state.consume(&accepted->reply, [](const RedisStreamRecord &) {
    return true;
  }) == 1U);
  auto duplicate_cursor = array(stream("cdc", entry("5-0", "a", "a")));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::DuplicateEntry, [&] {
    (void)state.consume(&duplicate_cursor->reply,
                        [](const RedisStreamRecord &) { return true; });
  }));
  auto behind_cursor = array(stream("cdc", entry("4-9", "a", "a")));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::ReorderedEntry, [&] {
    (void)state.consume(&behind_cursor->reply,
                        [](const RedisStreamRecord &) { return true; });
  }));
  BLACKBOX_CHECK(state.cursor("cdc") == "5-0");
}

void checkExactReplyAndFieldShape() {
  RedisXReadState state(twoStreams());
  std::size_t callbacks = 0U;
  const auto callback = [&](const RedisStreamRecord &) {
    ++callbacks;
    return true;
  };

  auto nil = nilReply();
  BLACKBOX_CHECK(state.consume(&nil->reply, callback) == 0U);
  BLACKBOX_CHECK(callbacks == 0U);

  auto empty = array();
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::MalformedReply, [&] {
    (void)state.consume(&empty->reply, callback);
  }));
  auto wrong_root = status("not-an-array");
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::MalformedReply, [&] {
    (void)state.consume(&wrong_root->reply, callback);
  }));
  auto server_error = errorReply("ERR test");
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::ServerError, [&] {
    (void)state.consume(&server_error->reply, callback);
  }));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::ConnectionFailed,
                         [&] { (void)state.consume(nullptr, callback); }));

  auto wrong_field_name = array(
      stream("blackbox:client:run", entry("8-0", "not-payload", "value")));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::FieldContractViolation, [&] {
    (void)state.consume(&wrong_field_name->reply, callback);
  }));
  auto multiple_fields =
      array(stream("cdc", array(bulk("1-0"), array(bulk("a"), bulk("b"),
                                                   bulk("c"), bulk("d")))));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::FieldContractViolation, [&] {
    (void)state.consume(&multiple_fields->reply, callback);
  }));
  auto non_bulk_field = array(
      stream("cdc", array(bulk("1-0"), array(status("field"), bulk("value")))));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::MalformedReply, [&] {
    (void)state.consume(&non_bulk_field->reply, callback);
  }));
  auto noncanonical_id = array(stream("cdc", entry("01-0", "key", "value")));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::InvalidStreamId, [&] {
    (void)state.consume(&noncanonical_id->reply, callback);
  }));
  BLACKBOX_CHECK(callbacks == 0U);
  BLACKBOX_CHECK(state.cursor("cdc") == "0-0");
}

void checkCursorAdvancesOnlyAfterCallbackSuccess() {
  RedisXReadState state({RedisStreamSpec{"cdc", "0-0", std::nullopt}});
  auto first_reply =
      array(stream("cdc", entry("1-0", "a", "a"), entry("2-0", "b", "b")));
  std::size_t callbacks = 0U;
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::CallbackRejected, [&] {
    (void)state.consume(&first_reply->reply, [&](const RedisStreamRecord &) {
      ++callbacks;
      return callbacks == 1U;
    });
  }));
  BLACKBOX_CHECK(callbacks == 2U);
  BLACKBOX_CHECK(state.cursor("cdc") == "1-0");

  auto retry = array(stream("cdc", entry("2-0", "b", "b")));
  BLACKBOX_CHECK(state.consume(&retry->reply, [](const RedisStreamRecord &) {
    return true;
  }) == 1U);
  BLACKBOX_CHECK(state.cursor("cdc") == "2-0");

  auto throwing = array(stream("cdc", entry("3-0", "c", "c")));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::CallbackFailed, [&] {
    (void)state.consume(&throwing->reply,
                        [](const RedisStreamRecord &) -> bool {
                          throw std::runtime_error("downstream failure");
                        });
  }));
  BLACKBOX_CHECK(state.cursor("cdc") == "2-0");
}

void checkReplyResourceLimits() {
  const auto callback = [](const RedisStreamRecord &) { return true; };

  RedisXReadState entry_limited({RedisStreamSpec{"cdc", "0-0", std::nullopt}},
                                RedisReaderLimits{.max_records_per_read = 10U,
                                                  .max_entry_bytes = 8U,
                                                  .max_batch_bytes = 32U});
  auto oversized_entry =
      array(stream("cdc", entry("1-0", "key", "0123456789")));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::ResourceExhausted, [&] {
    (void)entry_limited.consume(&oversized_entry->reply, callback);
  }));
  BLACKBOX_CHECK(entry_limited.cursor("cdc") == "0-0");

  RedisXReadState record_limited({RedisStreamSpec{"cdc", "0-0", std::nullopt}},
                                 RedisReaderLimits{.max_records_per_read = 1U,
                                                   .max_entry_bytes = 32U,
                                                   .max_batch_bytes = 64U});
  auto two_entries =
      array(stream("cdc", entry("1-0", "a", "a"), entry("2-0", "b", "b")));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::ResourceExhausted, [&] {
    (void)record_limited.consume(&two_entries->reply, callback);
  }));
  BLACKBOX_CHECK(record_limited.cursor("cdc") == "0-0");

  RedisXReadState batch_limited({RedisStreamSpec{"cdc", "0-0", std::nullopt}},
                                RedisReaderLimits{.max_records_per_read = 10U,
                                                  .max_entry_bytes = 8U,
                                                  .max_batch_bytes = 9U});
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::ResourceExhausted, [&] {
    (void)batch_limited.consume(&two_entries->reply, callback);
  }));
  BLACKBOX_CHECK(batch_limited.cursor("cdc") == "0-0");
}

void checkRetainedStorageAccounting() {
  const auto callback = [](const RedisStreamRecord &) { return true; };
  const RedisReaderLimits limits{.max_records_per_read = 1U,
                                 .max_entry_bytes = 16U,
                                 .max_batch_bytes = 512U};

  RedisXReadState short_name({RedisStreamSpec{"cdc", "0-0", std::nullopt}},
                             limits);
  auto short_reply = array(stream("cdc", entry("1-0", "k", "v")));
  BLACKBOX_CHECK(short_name.consume(&short_reply->reply, callback) == 1U);

  const std::string long_stream_name(200U, 's');
  RedisXReadState long_name(
      {RedisStreamSpec{long_stream_name, "0-0", std::nullopt}}, limits);
  auto long_reply = array(stream(long_stream_name, entry("1-0", "k", "v")));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::ResourceExhausted, [&] {
    (void)long_name.consume(&long_reply->reply, callback);
  }));
  BLACKBOX_CHECK(long_name.cursor(long_stream_name) == "0-0");

  RedisXReadState allocation_overhead(
      {RedisStreamSpec{"cdc", "0-0", std::nullopt}},
      RedisReaderLimits{.max_records_per_read = 2U,
                        .max_entry_bytes = 16U,
                        .max_batch_bytes = 512U});
  auto two_tiny_records =
      array(stream("cdc", entry("1-0", "k", "v"), entry("2-0", "k", "v")));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::ResourceExhausted, [&] {
    (void)allocation_overhead.consume(&two_tiny_records->reply, callback);
  }));
  BLACKBOX_CHECK(allocation_overhead.cursor("cdc") == "0-0");
}

void checkBoundedBlockingTransport() {
  const RedisReaderLimits limits{.max_records_per_read = 4U,
                                 .max_entry_bytes = 128U,
                                 .max_batch_bytes = 1024U};

  const std::string binary_field("k\0y", 3U);
  const std::string binary_value("v\0x", 3U);
  const std::string valid_reply = respArray(
      {respStream("cdc", {respEntry("1-0", binary_field, binary_value)})});
  SocketReplyRedisConnection valid_connection({valid_reply});
  BlockingRedisReader valid_reader(
      valid_connection.context(), {RedisStreamSpec{"cdc", "0-0", std::nullopt}},
      1U, std::chrono::milliseconds(0), limits);
  std::vector<RedisStreamRecord> delivered;
  BLACKBOX_CHECK(valid_reader.read_once([&](const RedisStreamRecord &record) {
    delivered.push_back(record);
    return true;
  }) == 1U);
  BLACKBOX_REQUIRE(delivered.size() == 1U);
  BLACKBOX_CHECK(delivered[0].stream_name == "cdc");
  BLACKBOX_CHECK(delivered[0].field_name == binary_field);
  BLACKBOX_CHECK(delivered[0].value == binary_value);
  BLACKBOX_CHECK(valid_reader.cursor("cdc") == "1-0");

  ScriptedRedisConnection closed_connection({valid_reply}, 0U, false, true);
  BlockingRedisReader closed_reader(
      closed_connection.context(),
      {RedisStreamSpec{"cdc", "0-0", std::nullopt}}, 1U,
      std::chrono::milliseconds(0), limits);
  std::size_t closed_callbacks = 0U;
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::ConnectionFailed, [&] {
    (void)closed_reader.read_once([&](const RedisStreamRecord &) {
      ++closed_callbacks;
      return true;
    });
  }));
  BLACKBOX_CHECK(closed_callbacks == 0U);
  BLACKBOX_CHECK(closed_reader.cursor("cdc") == "0-0");

  // The value terminator follows more than two complete 4 KiB reads, so both
  // bulk copying and CRLF validation must survive transport refills.
  std::string refill_value(8138U, 'v');
  refill_value[4090U] = '\0';
  const std::string refill_reply =
      respArray({respStream("cdc", {respEntry("1-0", "key", refill_value)})});
  SocketReplyRedisConnection refill_connection({refill_reply});
  BlockingRedisReader refill_reader(
      refill_connection.context(),
      {RedisStreamSpec{"cdc", "0-0", std::nullopt}}, 1U,
      std::chrono::milliseconds(0),
      RedisReaderLimits{.max_records_per_read = 1U,
                        .max_entry_bytes = 8192U,
                        .max_batch_bytes = 32U * 1024U});
  BLACKBOX_CHECK(refill_reader.read_once([&](const RedisStreamRecord &record) {
    return record.value == refill_value;
  }) == 1U);
  BLACKBOX_CHECK(refill_reader.cursor("cdc") == "1-0");

  std::string boundary_reply = respArray(
      {respStream("cdc", {respEntry("1-0", "key", std::string(4040U, 'b'))})});
  BLACKBOX_REQUIRE(boundary_reply.size() == 4096U);
  boundary_reply += "garbage";
  SocketReplyRedisConnection boundary_connection({boundary_reply});
  BlockingRedisReader boundary_reader(
      boundary_connection.context(),
      {RedisStreamSpec{"cdc", "0-0", std::nullopt}}, 1U,
      std::chrono::milliseconds(0),
      RedisReaderLimits{.max_records_per_read = 1U,
                        .max_entry_bytes = 8192U,
                        .max_batch_bytes = 16U * 1024U});
  std::size_t boundary_callbacks = 0U;
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::MalformedReply, [&] {
    (void)boundary_reader.read_once([&](const RedisStreamRecord &) {
      ++boundary_callbacks;
      return true;
    });
  }));
  BLACKBOX_CHECK(boundary_callbacks == 0U);
  BLACKBOX_CHECK(boundary_reader.cursor("cdc") == "0-0");

  const std::string count_violation =
      respArray({respStream("cdc", {respEntry("1-0", "key", "first"),
                                    respEntry("2-0", "key", "second")})});
  SocketReplyRedisConnection count_connection({count_violation});
  BlockingRedisReader count_reader(
      count_connection.context(), {RedisStreamSpec{"cdc", "0-0", std::nullopt}},
      1U, std::chrono::milliseconds(0), limits);
  std::size_t count_callbacks = 0U;
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::MalformedReply, [&] {
    (void)count_reader.read_once([&](const RedisStreamRecord &) {
      ++count_callbacks;
      return true;
    });
  }));
  BLACKBOX_CHECK(count_callbacks == 0U);
  BLACKBOX_CHECK(count_reader.cursor("cdc") == "0-0");

  const std::string id_header_only =
      "*1\r\n*2\r\n$3\r\ncdc\r\n*1\r\n*2\r\n$3\r\n";
  SocketReplyRedisConnection id_preflight_connection({id_header_only}, true);
  BlockingRedisReader id_preflight_reader(
      id_preflight_connection.context(),
      {RedisStreamSpec{"cdc", "0-0", std::nullopt}}, 1U,
      std::chrono::milliseconds(0),
      RedisReaderLimits{.max_records_per_read = 1U,
                        .max_entry_bytes = 2U,
                        .max_batch_bytes = 1024U});
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::ResourceExhausted, [&] {
    (void)id_preflight_reader.read_once(
        [](const RedisStreamRecord &) { return true; });
  }));

  const std::string field_header_only =
      "*1\r\n*2\r\n$3\r\ncdc\r\n*1\r\n*2\r\n$3\r\n1-0\r\n"
      "*2\r\n$400\r\n";
  SocketReplyRedisConnection field_preflight_connection({field_header_only},
                                                        true);
  BlockingRedisReader field_preflight_reader(
      field_preflight_connection.context(),
      {RedisStreamSpec{"cdc", "0-0", std::nullopt}}, 1U,
      std::chrono::milliseconds(0),
      RedisReaderLimits{.max_records_per_read = 1U,
                        .max_entry_bytes = 1024U,
                        .max_batch_bytes = 1024U});
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::ResourceExhausted, [&] {
    (void)field_preflight_reader.read_once(
        [](const RedisStreamRecord &) { return true; });
  }));

  const std::string malformed_tail =
      respArray({respStream("cdc", {respEntry("1-0", "key", "value"),
                                    respArray({respBulk("2-0")})})});
  SocketReplyRedisConnection malformed_connection({malformed_tail});
  BlockingRedisReader malformed_reader(
      malformed_connection.context(),
      {RedisStreamSpec{"cdc", "0-0", std::nullopt}}, 2U,
      std::chrono::milliseconds(0), limits);
  std::size_t malformed_callbacks = 0U;
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::MalformedReply, [&] {
    (void)malformed_reader.read_once([&](const RedisStreamRecord &) {
      ++malformed_callbacks;
      return true;
    });
  }));
  BLACKBOX_CHECK(malformed_callbacks == 0U);
  BLACKBOX_CHECK(malformed_reader.cursor("cdc") == "0-0");

  // The body is deliberately absent. The transport must reject the declared
  // length immediately instead of waiting for, or allocating, a 1 GB value.
  const std::string oversized_header =
      "*1\r\n*2\r\n$3\r\ncdc\r\n*1\r\n*2\r\n$3\r\n1-0\r\n"
      "*2\r\n$3\r\nkey\r\n$1000000000\r\n";
  SocketReplyRedisConnection oversized_connection({oversized_header}, true);
  BlockingRedisReader oversized_reader(
      oversized_connection.context(),
      {RedisStreamSpec{"cdc", "0-0", std::nullopt}}, 1U,
      std::chrono::milliseconds(0), limits);
  std::size_t callbacks = 0U;
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::ResourceExhausted, [&] {
    (void)oversized_reader.read_once([&](const RedisStreamRecord &) {
      ++callbacks;
      return true;
    });
  }));
  BLACKBOX_CHECK(callbacks == 0U);
  BLACKBOX_CHECK(oversized_reader.cursor("cdc") == "0-0");
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::ConnectionFailed, [&] {
    (void)oversized_reader.read_once(
        [](const RedisStreamRecord &) { return true; });
  }));
}

void checkBlockingWireTerminalReplies() {
  SocketReplyRedisConnection nil_connection({"*-1\r\n"});
  BlockingRedisReader nil_reader(nil_connection.context(),
                                 {RedisStreamSpec{"cdc", "0-0", std::nullopt}},
                                 1U, std::chrono::milliseconds(0));
  std::size_t callbacks = 0U;
  BLACKBOX_CHECK(nil_reader.read_once([&](const RedisStreamRecord &) {
    ++callbacks;
    return true;
  }) == 0U);
  BLACKBOX_CHECK(callbacks == 0U);
  BLACKBOX_CHECK(nil_reader.cursor("cdc") == "0-0");

  SocketReplyRedisConnection error_connection({"-ERR scripted failure\r\n"});
  BlockingRedisReader error_reader(
      error_connection.context(), {RedisStreamSpec{"cdc", "0-0", std::nullopt}},
      1U, std::chrono::milliseconds(0));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::ServerError, [&] {
    (void)error_reader.read_once([&](const RedisStreamRecord &) {
      ++callbacks;
      return true;
    });
  }));
  BLACKBOX_CHECK(callbacks == 0U);
  BLACKBOX_CHECK(error_reader.cursor("cdc") == "0-0");

  ScriptedRedisConnection recoverable_connection(
      {respArray({respStream("cdc", {respEntry("1-0", "key", "value")})})}, 1U);
  BlockingRedisReader recoverable_reader(
      recoverable_connection.context(),
      {RedisStreamSpec{"cdc", "0-0", std::nullopt}}, 1U,
      std::chrono::milliseconds(0));
  BLACKBOX_CHECK(recoverable_reader.read_once(
                     [](const RedisStreamRecord &) { return true; }) == 1U);
  BLACKBOX_CHECK(recoverable_reader.cursor("cdc") == "1-0");

  ScriptedRedisConnection oom_connection(
      {respArray({respStream("cdc", {respEntry("1-0", "key", "value")})})}, 0U,
      true);
  BlockingRedisReader oom_reader(oom_connection.context(),
                                 {RedisStreamSpec{"cdc", "0-0", std::nullopt}},
                                 1U, std::chrono::milliseconds(0));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::ResourceExhausted, [&] {
    (void)oom_reader.read_once([](const RedisStreamRecord &) { return true; });
  }));
  BLACKBOX_CHECK(oom_reader.cursor("cdc") == "0-0");
}

void checkBlockingCursorProgressionAcrossReads() {
  SocketReplyRedisConnection connection(
      {respArray({respStream("cdc", {respEntry("1-0", "key", "first")})}),
       respArray({respStream("cdc", {respEntry("2-0", "key", "second")})})});
  BlockingRedisReader reader(connection.context(),
                             {RedisStreamSpec{"cdc", "0-0", std::nullopt}}, 1U,
                             std::chrono::milliseconds(0));

  std::vector<std::string> values;
  const auto callback = [&](const RedisStreamRecord &record) {
    values.push_back(record.value);
    return true;
  };
  BLACKBOX_CHECK(reader.read_once(callback) == 1U);
  BLACKBOX_CHECK(reader.cursor("cdc") == "1-0");
  BLACKBOX_CHECK(reader.read_once(callback) == 1U);
  BLACKBOX_CHECK(reader.cursor("cdc") == "2-0");
  BLACKBOX_CHECK(values == std::vector<std::string>({"first", "second"}));

  connection.finish();
  const std::vector<std::string> &commands = connection.commands();
  BLACKBOX_REQUIRE(commands.size() == 2U);
  BLACKBOX_CHECK(commands[0].find("\r\n$3\r\n0-0\r\n") != std::string::npos);
  BLACKBOX_CHECK(commands[1].find("\r\n$3\r\n1-0\r\n") != std::string::npos);
}

void checkBorrowedTransportContaminationIsTerminal() {
  const std::string reply =
      respArray({respStream("cdc", {respEntry("1-0", "key", "value")})});
  const auto callback = [](const RedisStreamRecord &) { return true; };

  ScriptedRedisConnection queued_connection({reply});
  queued_connection.queuePeerBytes("stale");
  BlockingRedisReader queued_reader(
      queued_connection.context(),
      {RedisStreamSpec{"cdc", "0-0", std::nullopt}}, 1U,
      std::chrono::milliseconds(0));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::MalformedReply,
                         [&] { (void)queued_reader.read_once(callback); }));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::ConnectionFailed,
                         [&] { (void)queued_reader.read_once(callback); }));
  BLACKBOX_CHECK(queued_connection.commands().empty());

  ScriptedRedisConnection closed_connection({reply});
  closed_connection.closePeerWrite();
  BlockingRedisReader closed_reader(
      closed_connection.context(),
      {RedisStreamSpec{"cdc", "0-0", std::nullopt}}, 1U,
      std::chrono::milliseconds(0));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::ConnectionFailed,
                         [&] { (void)closed_reader.read_once(callback); }));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::ConnectionFailed,
                         [&] { (void)closed_reader.read_once(callback); }));
  BLACKBOX_CHECK(closed_connection.commands().empty());
}

void checkBlockingCallbackValidatedBeforeIo() {
  redisContext unusable_context{};
  BlockingRedisReader reader(&unusable_context,
                             {RedisStreamSpec{"cdc", "0-0", std::nullopt}}, 1U,
                             std::chrono::milliseconds(0));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::InvalidConfiguration,
                         [&] { (void)reader.read_once({}); }));
  BLACKBOX_CHECK(rejects(RedisReaderErrorCode::InvalidConfiguration, [&] {
    (void)reader.read_once([](const RedisStreamRecord &) { return true; });
  }));
}

} // namespace

int runRedisReaderTests() {
  checkConfigurationAndCommand();
  checkValidBinarySafeMultiStreamReply();
  checkWholeReplyValidationBeforeDelivery();
  checkDuplicateAndReorderedEvidence();
  checkExactReplyAndFieldShape();
  checkCursorAdvancesOnlyAfterCallbackSuccess();
  checkReplyResourceLimits();
  checkRetainedStorageAccounting();
  checkBoundedBlockingTransport();
  checkBlockingWireTerminalReplies();
  checkBlockingCursorProgressionAcrossReads();
  checkBorrowedTransportContaminationIsTerminal();
  checkBlockingCallbackValidatedBeforeIo();
  return 0;
}

#ifdef BLACKBOX_REDIS_READER_STANDALONE
int main() {
  runRedisReaderTests();
  return blackbox::test::failures == 0 ? 0 : 1;
}
#endif
