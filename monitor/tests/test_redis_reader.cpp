#include "blackbox/redis_reader.h"
#include "test_support.h"

#include <hiredis/hiredis.h>

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using blackbox::BlockingRedisReader;
using blackbox::RedisReaderError;
using blackbox::RedisReaderErrorCode;
using blackbox::RedisReaderLimits;
using blackbox::RedisStreamRecord;
using blackbox::RedisStreamSpec;
using blackbox::RedisXReadState;

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

} // namespace

int runRedisReaderTests() {
  checkConfigurationAndCommand();
  checkValidBinarySafeMultiStreamReply();
  checkWholeReplyValidationBeforeDelivery();
  checkDuplicateAndReorderedEvidence();
  checkExactReplyAndFieldShape();
  checkCursorAdvancesOnlyAfterCallbackSuccess();
  checkReplyResourceLimits();
  return 0;
}

#ifdef BLACKBOX_REDIS_READER_STANDALONE
int main() {
  runRedisReaderTests();
  return blackbox::test::failures == 0 ? 0 : 1;
}
#endif
