#include "strict_json.h"

#include <map>
#include <set>
#include <string>

namespace blackbox::detail {

nlohmann::json parseStrictJson(const std::string_view input) {
  std::map<int, std::set<std::string>> object_keys;
  const auto callback =
      [&object_keys](const int depth, const nlohmann::json::parse_event_t event,
                     nlohmann::json &parsed) {
        switch (event) {
        case nlohmann::json::parse_event_t::object_start:
          object_keys[depth + 1].clear();
          break;
        case nlohmann::json::parse_event_t::key: {
          auto &keys = object_keys[depth];
          const auto [unused, inserted] =
              keys.insert(parsed.get<std::string>());
          (void)unused;
          if (!inserted) {
            throw StrictJsonError("duplicate JSON object key: " +
                                  parsed.get<std::string>());
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
