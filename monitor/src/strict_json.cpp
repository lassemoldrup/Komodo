#include "strict_json.h"

#include <string>
#include <utility>
#include <vector>

namespace blackbox::detail {
namespace {

using Json = nlohmann::json;

// Builds the one DOM required by callers directly from SAX events while
// enforcing the ambiguity and resource checks that the callback-based DOM
// parser previously performed through temporary JSON values and key sets.
class StrictSaxDomParser final {
public:
  using number_integer_t = Json::number_integer_t;
  using number_unsigned_t = Json::number_unsigned_t;
  using number_float_t = Json::number_float_t;
  using string_t = Json::string_t;
  using binary_t = Json::binary_t;

  StrictSaxDomParser(Json &result, const StrictJsonLimits limits)
      : result_(result), limits_(limits) {}

  bool null() {
    recordEvent(stack_.size());
    append(Json(nullptr));
    return true;
  }

  bool boolean(const bool value) {
    recordEvent(stack_.size());
    append(Json(value));
    return true;
  }

  bool number_integer(const number_integer_t value) {
    recordEvent(stack_.size());
    append(Json(value));
    return true;
  }

  bool number_unsigned(const number_unsigned_t value) {
    recordEvent(stack_.size());
    append(Json(value));
    return true;
  }

  bool number_float(const number_float_t value, const string_t &) {
    recordEvent(stack_.size());
    append(Json(value));
    return true;
  }

  bool string(string_t &value) {
    recordEvent(stack_.size());
    recordString(value);
    append(Json(std::move(value)));
    return true;
  }

  bool binary(binary_t &value) {
    recordEvent(stack_.size());
    append(Json(std::move(value)));
    return true;
  }

  bool start_object(const std::size_t) {
    recordEvent(stack_.size());
    stack_.push_back(append(Json::object()));
    return true;
  }

  bool key(string_t &value) {
    recordEvent(stack_.size());
    recordString(value);
    auto &object = *stack_.back();
    if (object.contains(value)) {
      throw StrictJsonError("duplicate JSON object key: " + value);
    }
    if (object.size() == limits_.max_object_members) {
      throw StrictJsonLimitError("JSON object member count exceeds its limit");
    }
    current_key_ = std::move(value);
    return true;
  }

  bool end_object() {
    recordEvent(stack_.size() - 1U);
    stack_.pop_back();
    return true;
  }

  bool start_array(const std::size_t) {
    recordEvent(stack_.size());
    stack_.push_back(append(Json::array()));
    return true;
  }

  bool end_array() {
    recordEvent(stack_.size() - 1U);
    stack_.pop_back();
    return true;
  }

  bool parse_error(const std::size_t, const std::string &,
                   const nlohmann::detail::exception &error) {
    throw StrictJsonError(error.what());
  }

private:
  Json *append(Json value) {
    if (stack_.empty()) {
      result_ = std::move(value);
      return &result_;
    }
    auto &parent = *stack_.back();
    if (parent.is_array()) {
      parent.push_back(std::move(value));
      return &parent.back();
    }
    auto [position, inserted] =
        parent.emplace(std::move(current_key_), std::move(value));
    if (!inserted) {
      throw StrictJsonError("duplicate JSON object key");
    }
    return &position.value();
  }

  void recordEvent(const std::size_t depth) {
    if (depth > limits_.max_depth) {
      throw StrictJsonLimitError("JSON nesting depth exceeds its limit");
    }
    if (event_count_ == limits_.max_events) {
      throw StrictJsonLimitError("JSON event count exceeds its limit");
    }
    ++event_count_;
  }

  void recordString(const std::string_view value) {
    if (string_bytes_ > limits_.max_string_bytes ||
        value.size() > limits_.max_string_bytes - string_bytes_) {
      throw StrictJsonLimitError("JSON string bytes exceed their limit");
    }
    string_bytes_ += value.size();
  }

  Json &result_;
  StrictJsonLimits limits_;
  std::vector<Json *> stack_;
  std::string current_key_;
  std::size_t event_count_ = 0;
  std::size_t string_bytes_ = 0;
};

} // namespace

nlohmann::json parseStrictJson(const std::string_view input,
                               const StrictJsonLimits limits) {
  try {
    Json result;
    StrictSaxDomParser parser(result, limits);
    if (!Json::sax_parse(input.begin(), input.end(), &parser,
                         Json::input_format_t::json, true, false)) {
      throw StrictJsonError("JSON parser rejected input");
    }
    return result;
  } catch (const StrictJsonError &) {
    throw;
  } catch (const nlohmann::json::exception &error) {
    throw StrictJsonError(error.what());
  }
}

} // namespace blackbox::detail
