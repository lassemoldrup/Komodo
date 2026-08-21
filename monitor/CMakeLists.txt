cmake_minimum_required(VERSION 3.25)

project(blackbox_monitor LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(BLACKBOX_MONITOR_BUILD_TESTING "Build black-box monitor tests" ON)

set(CO_CHECKER_ROOT "" CACHE PATH "Path to the co-checker source checkout")
if(NOT CO_CHECKER_ROOT)
  message(FATAL_ERROR "CO_CHECKER_ROOT must name the co-checker source checkout")
endif()
cmake_path(ABSOLUTE_PATH CO_CHECKER_ROOT NORMALIZE
           OUTPUT_VARIABLE BLACKBOX_CO_CHECKER_ROOT)
if(NOT EXISTS "${BLACKBOX_CO_CHECKER_ROOT}/online/CMakeLists.txt")
  message(FATAL_ERROR
          "CO_CHECKER_ROOT does not contain online/CMakeLists.txt: ${BLACKBOX_CO_CHECKER_ROOT}")
endif()
set(CO_CHECKER_ONLINE_BUILD_TESTING OFF CACHE BOOL "" FORCE)
add_subdirectory("${BLACKBOX_CO_CHECKER_ROOT}/online"
                 "${CMAKE_CURRENT_BINARY_DIR}/co-checker-online"
                 EXCLUDE_FROM_ALL)

find_package(PkgConfig REQUIRED)
pkg_check_modules(HIREDIS REQUIRED IMPORTED_TARGET hiredis=1.2.0)
find_package(nlohmann_json 3.11.3 EXACT CONFIG REQUIRED)
find_package(OpenSSL REQUIRED COMPONENTS Crypto)

add_library(blackbox_monitor
  src/backend_adapter.cpp
  src/benchmark_adapter.cpp
  src/canonical_tpcc_codec.cpp
  src/canonical_ycsb_codec.cpp
  src/cdc_error.cpp
  src/cdc_types.cpp
  src/checker_adapter.cpp
  src/client_txn.cpp
  src/debezium_assembler.cpp
  src/monitor_error.cpp
  src/protocol_records.cpp
  src/redis_reader.cpp
  src/run_controller.cpp
  src/strict_json.cpp
  src/transaction_joiner.cpp
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
    nlohmann_json::nlohmann_json
    co_checker_online
  PRIVATE
    OpenSSL::Crypto
)

add_executable(blackbox_cdc_replay
  tools/cdc_replay.cpp
)

add_executable(blackbox_cdc_ingest
  tools/cdc_ingest.cpp
)
add_executable(blackbox_prefix_ingest
  tools/prefix_ingest.cpp
)
add_executable(blackbox_run_monitor
  tools/monitor.cpp
)
target_link_libraries(blackbox_cdc_ingest PRIVATE blackbox_monitor)
target_link_libraries(blackbox_prefix_ingest PRIVATE blackbox_monitor)
target_link_libraries(blackbox_run_monitor PRIVATE blackbox_monitor)
target_compile_options(blackbox_cdc_ingest
  PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -Wconversion
    -Wsign-conversion
)
target_link_libraries(blackbox_cdc_replay PRIVATE blackbox_monitor)
target_compile_options(blackbox_cdc_replay
  PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -Wconversion
    -Wsign-conversion
)
target_compile_options(blackbox_prefix_ingest
  PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -Wconversion
    -Wsign-conversion
)
target_compile_options(blackbox_run_monitor
  PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -Wconversion
    -Wsign-conversion
)
if(BLACKBOX_MONITOR_BUILD_TESTING)
  include(CTest)
  find_package(Python3 REQUIRED COMPONENTS Interpreter)

  add_executable(blackbox_monitor_tests
    tests/test_backend_adapter.cpp
    tests/test_benchmark_adapter.cpp
    tests/test_canonical_tpcc_codec.cpp
    tests/test_canonical_ycsb_codec.cpp
    tests/test_checker_adapter.cpp
    tests/test_client_txn.cpp
    tests/test_debezium_assembler.cpp
    tests/test_main.cpp
    tests/test_protocol_records.cpp
    tests/test_redis_reader.cpp
    tests/test_run_controller.cpp
    tests/test_transaction_joiner.cpp
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
      BLACKBOX_PROTOCOL_ADAPTER_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../protocol/adapters"
      BLACKBOX_PROTOCOL_BACKEND_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../protocol/backends"
      BLACKBOX_PROTOCOL_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../protocol/fixtures"
      BLACKBOX_PROTOCOL_EXAMPLE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../protocol/examples"
      BLACKBOX_CDC_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures"
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
  add_test(
    NAME blackbox_monitor_cli_fail_closed
    COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_monitor_cli.py
            --monitor $<TARGET_FILE:blackbox_run_monitor>
  )
  add_test(
    NAME blackbox_cdc_transaction_fixture_checksum
    COMMAND ${CMAKE_COMMAND} -E sha256sum
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/mariadb_debezium_3_6_1_lifecycle.json
  )
  set_tests_properties(blackbox_cdc_transaction_fixture_checksum PROPERTIES
    PASS_REGULAR_EXPRESSION "dc202702ce0c1d39aa945d38c4f772b5202335e36378ff59f8d8186505b81eeb"
  )
  add_test(
    NAME blackbox_cdc_full_lifecycle_fixture_checksum
    COMMAND ${CMAKE_COMMAND} -E sha256sum
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/mariadb_debezium_3_6_1_full_lifecycle.json
  )
  set_tests_properties(blackbox_cdc_full_lifecycle_fixture_checksum PROPERTIES
    FIXTURES_SETUP captured_mariadb_full_lifecycle_fixture
    PASS_REGULAR_EXPRESSION "6148236e8806c8fc6f3693a3390771a8e905ffbc603d403a3b1a858387b48185"
  )
  add_test(
    NAME blackbox_cdc_snapshot_schema_fixture_checksum
    COMMAND ${CMAKE_COMMAND} -E sha256sum
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/mariadb_debezium_3_6_1_snapshot_schema.json
  )
  set_tests_properties(blackbox_cdc_snapshot_schema_fixture_checksum PROPERTIES
    FIXTURES_SETUP captured_mariadb_snapshot_schema_fixture
    PASS_REGULAR_EXPRESSION "4be5e5dbb70beffd2acb558708e52482d348a0b0e19983dc34c4e775f38f1301"
  )
  add_test(
    NAME blackbox_tpcc_typed_cdc_fixture_checksum
    COMMAND ${CMAKE_COMMAND} -E sha256sum
            ${CMAKE_CURRENT_SOURCE_DIR}/../protocol/fixtures/mariadb_tpcc_typed_rows.json
  )
  set_tests_properties(blackbox_tpcc_typed_cdc_fixture_checksum PROPERTIES
    PASS_REGULAR_EXPRESSION "fa4f72247f42eb5062082a964b118acdbd54a99c991c7ae4aeda99bcc267bb82"
  )
  add_test(
    NAME blackbox_tpcc_transaction_fixture_checksum
    COMMAND ${CMAKE_COMMAND} -E sha256sum
            ${CMAKE_CURRENT_SOURCE_DIR}/../protocol/fixtures/mariadb_tpcc_transactions.json
  )
  set_tests_properties(blackbox_tpcc_transaction_fixture_checksum PROPERTIES
    PASS_REGULAR_EXPRESSION "1a527bbda98d921d24a7e4a1751be9602e27341f56f8f2fc995c0b97bd5322b7"
  )
  add_test(
    NAME blackbox_tpcc_client_fixture_checksum
    COMMAND ${CMAKE_COMMAND} -E sha256sum
            ${CMAKE_CURRENT_SOURCE_DIR}/../protocol/fixtures/mariadb_tpcc_client_txns.json
  )
  set_tests_properties(blackbox_tpcc_client_fixture_checksum PROPERTIES
    PASS_REGULAR_EXPRESSION "4089177ea3460074819ef1599147266dca115515df9c845c279832a17e5762d3"
  )
  add_test(
    NAME blackbox_cdc_replay_snapshot_schema_fixture
    COMMAND blackbox_cdc_replay
            --input
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/mariadb_debezium_3_6_1_snapshot_schema.json
            --run-id
            084cc5a1-3cbe-4fef-bb5c-43559a1c13a0
  )
  set_tests_properties(blackbox_cdc_replay_snapshot_schema_fixture PROPERTIES
    FIXTURES_REQUIRED captured_mariadb_snapshot_schema_fixture
    PASS_REGULAR_EXPRESSION "\"completed_transactions\": 0"
  )
  add_test(
    NAME blackbox_cdc_replay_fixture
    COMMAND blackbox_cdc_replay
            --input
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/mariadb_debezium_3_6_1_full_lifecycle.json
            --run-id
            084cc5a1-3cbe-4fef-bb5c-43559a1c13a0
            --backend
            mariadb
            --benchmark
            ycsb
  )
  set_tests_properties(blackbox_cdc_replay_fixture PROPERTIES
    FIXTURES_REQUIRED captured_mariadb_full_lifecycle_fixture
    PASS_REGULAR_EXPRESSION "\"completed_transactions\": 11"
  )
  add_test(
    NAME blackbox_tpcc_cdc_replay_contract
    COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_tpcc_cdc_replay.py
            --replay $<TARGET_FILE:blackbox_cdc_replay>
            --fixture
            ${CMAKE_CURRENT_SOURCE_DIR}/../protocol/fixtures/mariadb_tpcc_transactions.json
  )
endif()
