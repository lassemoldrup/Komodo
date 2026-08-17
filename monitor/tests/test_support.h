#pragma once

#include <iostream>
#include <string_view>

namespace blackbox::test {

inline int failures = 0;

inline void check(const bool condition, const std::string_view expression,
                  const std::string_view file, const int line) {
  if (condition) {
    return;
  }
  std::cerr << file << ':' << line << ": check failed: " << expression << '\n';
  ++failures;
}

} // namespace blackbox::test

#define BLACKBOX_CHECK(expression)                                             \
  ::blackbox::test::check(static_cast<bool>(expression), #expression,          \
                          __FILE__, __LINE__)

#define BLACKBOX_REQUIRE(expression)                                           \
  do {                                                                         \
    if (!static_cast<bool>(expression)) {                                      \
      ::blackbox::test::check(false, #expression, __FILE__, __LINE__);         \
      return;                                                                  \
    }                                                                          \
  } while (false)
