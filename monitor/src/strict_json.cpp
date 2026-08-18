#include "strict_json.h"

#include <map>
#include <set>
#include <string>

namespace blackbox::detail {

nlohmann::json parseStrictJson(const std::string_view input,
                               const StrictJsonLimits limits) {
  std::map<int, std::set<std::string>> object_keys;
  std::size_t event_count = 0;
  std::size_t string_bytes = 0;
  const auto callback = [&object_keys, &event_count, &string_bytes,
                         limits](const int depth,
                                 const nlohmann::json::parse_event_t event,
                                 nlohmann::json &parsed) {
    if (depth < 0 || static_cast<std::size_t>(depth) > limits.max_depth) {
      throw StrictJsonLimitError("JSON nesting depth exceeds its limit");
    }
    if (event_count == limits.max_events) {
      throw StrictJsonLimitError("JSON event count exceeds its limit");
    }
    ++event_count;
    if ((event == nlohmann::json::parse_event_t::key ||
         event == nlohmann::json::parse_event_t::value) &&
        parsed.is_string()) {
      const auto size = parsed.get_ref<const std::string &>().size();
      if (string_bytes > limits.max_string_bytes ||
          size > limits.max_string_bytes - string_bytes) {
        throw StrictJsonLimitError("JSON string bytes exceed their limit");
      }
      string_bytes += size;
    }
    switch (event) {
    case nlohmann::json::parse_event_t::object_start:
      object_keys[depth + 1].clear();
      break;
    case nlohmann::json::parse_event_t::key: {
      auto &keys = object_keys[depth];
      const auto [unused, inserted] = keys.insert(parsed.get<std::string>());
      (void)unused;
      if (!inserted) {
        throw StrictJsonError("duplicate JSON object key: " +
                              parsed.get<std::string>());
      }
      if (keys.size() > limits.max_object_members) {
        throw StrictJsonLimitError(
            "JSON object member count exceeds its limit");
      }
      break;
    }
    case nlohmann::json::parse_event_t::object_end:
      object_keys.erase(depth + 1);
      break;
    case nlohmann::json::parse_event_t::array_start:
    case nlohmann::json::parse_event_t::array_end:
    case nlohmann::json::parse_event_t::value:
      break;
    }
    return true;
  };

  try {
    return nlohmann::json::parse(input.begin(), input.end(), callback, true,
                                 false);
  } catch (const StrictJsonError &) {
    throw;
  } catch (const nlohmann::json::exception &error) {
    throw StrictJsonError(error.what());
  }
}

} // namespace blackbox::detail
