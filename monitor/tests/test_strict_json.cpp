#include "test_support.h"

#include "../src/strict_json.h"

#include <nlohmann/json.hpp>

#include <string_view>

namespace {

using blackbox::detail::StrictJsonError;
using blackbox::detail::StrictJsonLimitError;
using blackbox::detail::StrictJsonLimits;

enum class ParseOutcome { Accepted, Rejected, LimitRejected };

[[nodiscard]] ParseOutcome
outcome(const std::string_view input,
        const StrictJsonLimits limits = StrictJsonLimits{}) {
  try {
    (void)blackbox::detail::parseStrictJson(input, limits);
    return ParseOutcome::Accepted;
  } catch (const StrictJsonLimitError &) {
    return ParseOutcome::LimitRejected;
  } catch (const StrictJsonError &) {
    return ParseOutcome::Rejected;
  }
}

void testDomConstruction() {
  constexpr std::string_view input =
      R"({"array":[null,true,false,-1,2,3.5,"value"],"nested":{"key":"text"}})";
  const auto parsed = blackbox::detail::parseStrictJson(input);
  BLACKBOX_CHECK(parsed == nlohmann::json::parse(input));
  BLACKBOX_CHECK(parsed.at("array").at(3).is_number_integer());
  BLACKBOX_CHECK(parsed.at("array").at(4).is_number_unsigned());
  BLACKBOX_CHECK(parsed.at("array").at(5).is_number_float());
}

void testAmbiguityAndSyntaxRejection() {
  BLACKBOX_CHECK(outcome(R"({"key":1,"key":2})") == ParseOutcome::Rejected);
  BLACKBOX_CHECK(outcome(R"({"outer":{"key":1,"key":2}})") ==
                 ParseOutcome::Rejected);
  BLACKBOX_CHECK(outcome(R"({"left":{"key":1},"right":{"key":2}})") ==
                 ParseOutcome::Accepted);
  BLACKBOX_CHECK(outcome(R"({"unterminated":)") == ParseOutcome::Rejected);

  auto limits = StrictJsonLimits{};
  limits.max_object_members = 1U;
  BLACKBOX_CHECK(outcome(R"({"key":1,"key":2})", limits) ==
                 ParseOutcome::Rejected);
}

void testDepthAndEventLimits() {
  auto limits = StrictJsonLimits{};
  limits.max_depth = 0U;
  BLACKBOX_CHECK(outcome("1", limits) == ParseOutcome::Accepted);
  BLACKBOX_CHECK(outcome("{}", limits) == ParseOutcome::Accepted);
  BLACKBOX_CHECK(outcome(R"({"key":1})", limits) ==
                 ParseOutcome::LimitRejected);

  limits.max_depth = 1U;
  BLACKBOX_CHECK(outcome(R"({"key":{}})", limits) == ParseOutcome::Accepted);
  BLACKBOX_CHECK(outcome(R"({"key":{"nested":1}})", limits) ==
                 ParseOutcome::LimitRejected);

  limits = StrictJsonLimits{};
  limits.max_events = 4U;
  BLACKBOX_CHECK(outcome(R"({"key":1})", limits) == ParseOutcome::Accepted);
  limits.max_events = 3U;
  BLACKBOX_CHECK(outcome(R"({"key":1})", limits) ==
                 ParseOutcome::LimitRejected);
}

void testStringAndMemberLimits() {
  auto limits = StrictJsonLimits{};
  limits.max_string_bytes = 3U;
  BLACKBOX_CHECK(outcome(R"({"a":"bc"})", limits) == ParseOutcome::Accepted);
  limits.max_string_bytes = 2U;
  BLACKBOX_CHECK(outcome(R"({"a":"bc"})", limits) ==
                 ParseOutcome::LimitRejected);

  limits.max_string_bytes = 4U;
  BLACKBOX_CHECK(outcome(R"({"\u00e9":"\u00df"})", limits) ==
                 ParseOutcome::Accepted);
  limits.max_string_bytes = 3U;
  BLACKBOX_CHECK(outcome(R"({"\u00e9":"\u00df"})", limits) ==
                 ParseOutcome::LimitRejected);

  limits = StrictJsonLimits{};
  limits.max_object_members = 1U;
  BLACKBOX_CHECK(outcome(R"({"key":1})", limits) == ParseOutcome::Accepted);
  BLACKBOX_CHECK(outcome(R"({"key":1,"other":2})", limits) ==
                 ParseOutcome::LimitRejected);
}

} // namespace

int runStrictJsonTests() {
  testDomConstruction();
  testAmbiguityAndSyntaxRejection();
  testDepthAndEventLimits();
  testStringAndMemberLimits();
  return blackbox::test::failures;
}
