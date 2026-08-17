cmake_minimum_required(VERSION 3.25)

project(blackbox_monitor LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(BLACKBOX_MONITOR_BUILD_TESTING "Build black-box monitor tests" ON)

find_package(PkgConfig REQUIRED)
pkg_check_modules(HIREDIS REQUIRED IMPORTED_TARGET hiredis=1.2.0)
find_package(nlohmann_json 3.11.3 EXACT CONFIG REQUIRED)

add_library(blackbox_monitor
  src/canonical_ycsb_codec.cpp
  src/redis_reader.cpp
)
target_include_directories(blackbox_monitor
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_compile_options(blackbox_monitor
  PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -Wconversion
    -Wsign-conversion
)
target_link_libraries(blackbox_monitor
  PUBLIC
    PkgConfig::HIREDIS
)

if(BLACKBOX_MONITOR_BUILD_TESTING)
  include(CTest)

  add_executable(blackbox_monitor_tests
    tests/test_canonical_ycsb_codec.cpp
    tests/test_main.cpp
    tests/test_redis_reader.cpp
  )
  target_link_libraries(blackbox_monitor_tests
    PRIVATE
      blackbox_monitor
      nlohmann_json::nlohmann_json
  )
  target_include_directories(blackbox_monitor_tests
    PRIVATE
      ${CMAKE_CURRENT_SOURCE_DIR}/tests
  )
  target_compile_definitions(blackbox_monitor_tests
    PRIVATE
      BLACKBOX_PROTOCOL_GOLDEN_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../protocol/golden"
  )
  target_compile_options(blackbox_monitor_tests
    PRIVATE
      -Wall
      -Wextra
      -Wpedantic
      -Wconversion
      -Wsign-conversion
  )
  add_test(NAME blackbox_monitor_tests COMMAND blackbox_monitor_tests)
endif()
