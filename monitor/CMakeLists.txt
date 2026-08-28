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
find_package(simdjson 3.6.4 EXACT CONFIG REQUIRED)
find_package(OpenSSL REQUIRED COMPONENTS Crypto)

add_library(blackbox_monitor
  src/backend_adapter.cpp
  src/benchmark_adapter.cpp
  src/canonical_tpcc_codec.cpp
  src/canonical_smallbank_codec.cpp
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
    simdjson::simdjson
    co_checker_online
  PRIVATE
    OpenSSL::Crypto
)

add_executable(blackbox_cdc_replay
  tools/cdc_replay.cpp
)
add_executable(blackbox_cdc_codec_replay
  tools/cdc_codec_replay.cpp
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
add_executable(blackbox_evidence_replay
  tools/evidence_replay.cpp
)
target_link_libraries(blackbox_cdc_ingest PRIVATE blackbox_monitor)
target_link_libraries(blackbox_prefix_ingest PRIVATE blackbox_monitor)
target_link_libraries(blackbox_run_monitor PRIVATE blackbox_monitor)
target_link_libraries(blackbox_evidence_replay PRIVATE blackbox_monitor)
target_compile_options(blackbox_cdc_ingest
  PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -Wconversion
    -Wsign-conversion
)
target_link_libraries(blackbox_cdc_replay PRIVATE blackbox_monitor)
target_link_libraries(blackbox_cdc_codec_replay PRIVATE blackbox_monitor)
target_compile_options(blackbox_cdc_codec_replay
  PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -Wconversion
    -Wsign-conversion
)
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
target_compile_options(blackbox_evidence_replay
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
    tests/test_canonical_smallbank_codec.cpp
    tests/test_canonical_ycsb_codec.cpp
    tests/test_checker_adapter.cpp
    tests/test_client_txn.cpp
    tests/test_debezium_assembler.cpp
    tests/test_main.cpp
    tests/test_protocol_records.cpp
    tests/test_redis_reader.cpp
    tests/test_run_controller.cpp
    tests/test_strict_json.cpp
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
    PASS_REGULAR_EXPRESSION "aa27fb89de21508c404242f8234eb40fc432de1d3751035a59c3787b01896325"
  )
  add_test(
    NAME blackbox_cdc_full_lifecycle_fixture_checksum
    COMMAND ${CMAKE_COMMAND} -E sha256sum
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/mariadb_debezium_3_6_1_full_lifecycle.json
  )
  set_tests_properties(blackbox_cdc_full_lifecycle_fixture_checksum PROPERTIES
    FIXTURES_SETUP captured_mariadb_full_lifecycle_fixture
    PASS_REGULAR_EXPRESSION "2b2a9f9dc25fc32467b72fee76c89ad80fff3cecc1449442527358b53b06d94a"
  )
  add_test(
    NAME blackbox_cdc_snapshot_schema_fixture_checksum
    COMMAND ${CMAKE_COMMAND} -E sha256sum
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/mariadb_debezium_3_6_1_snapshot_schema.json
  )
  set_tests_properties(blackbox_cdc_snapshot_schema_fixture_checksum PROPERTIES
    FIXTURES_SETUP captured_mariadb_snapshot_schema_fixture
    PASS_REGULAR_EXPRESSION "24d1be95f4fb45811bf31d36dee325b4f3bc840471d732e37a9182aae0295306"
  )
  add_test(
    NAME blackbox_yugabytedb_raw_fixture_checksum
    COMMAND ${CMAKE_COMMAND} -E sha256sum
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/yugabytedb_pgoutput_2_5_2_raw_cross_tablet.json
  )
  set_tests_properties(blackbox_yugabytedb_raw_fixture_checksum PROPERTIES
    PASS_REGULAR_EXPRESSION "7c3eecf40252fc12ef074445b3f5a1d7384b2a3db81e304589af23c5b5629579"
  )
  add_test(
    NAME blackbox_yugabytedb_lifecycle_fixture_checksum
    COMMAND ${CMAKE_COMMAND} -E sha256sum
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/yugabytedb_pgoutput_2_5_2_lifecycle.json
  )
  add_test(
    NAME blackbox_oracle_smallbank_fixture_checksum
    COMMAND ${CMAKE_COMMAND} -E sha256sum
      ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/oracle_logminer_3_6_1_smallbank.json
  )
  set_tests_properties(blackbox_oracle_smallbank_fixture_checksum PROPERTIES
    PASS_REGULAR_EXPRESSION "59234a0dd2c115dde046ad3531160de5f8d094532403e9dbc947fced25e23f00"
  )
  set_tests_properties(blackbox_yugabytedb_lifecycle_fixture_checksum PROPERTIES
    PASS_REGULAR_EXPRESSION "afd4da9f220ca46cd6a358bea7c4620773c272efccfaea5fe1bbcaae270b5735"
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
    PASS_REGULAR_EXPRESSION "517d3f63e8b3ca9e006bd518a68ca051b7b52c78272655dd29b490f4de249657"
  )
  add_test(
    NAME blackbox_tpcc_client_fixture_checksum
    COMMAND ${CMAKE_COMMAND} -E sha256sum
            ${CMAKE_CURRENT_SOURCE_DIR}/../protocol/fixtures/mariadb_tpcc_client_txns.json
  )
  set_tests_properties(blackbox_tpcc_client_fixture_checksum PROPERTIES
    PASS_REGULAR_EXPRESSION "e515b82f6896a31fe1b7265c26b5bdcc0467962ded92f055e174883f1f1e6cad"
  )
  add_test(
    NAME blackbox_cdc_replay_snapshot_schema_fixture
    COMMAND blackbox_cdc_replay
            --input
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/mariadb_debezium_3_6_1_snapshot_schema.json
            --run-id
            084cc5a1-3cbe-4fef-bb5c-43559a1c13a0
            --backend
            mariadb
            --benchmark
            ycsb
            --backend-source-id
            mariadb-main
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
            --backend-source-id
            mariadb-main
  )
  set_tests_properties(blackbox_cdc_replay_fixture PROPERTIES
    FIXTURES_REQUIRED captured_mariadb_full_lifecycle_fixture
    PASS_REGULAR_EXPRESSION "\"completed_transactions\": 11"
  )
  add_test(
    NAME blackbox_cdc_replay_yugabytedb_fixture
    COMMAND blackbox_cdc_replay
            --input
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/yugabytedb_pgoutput_2_5_2_lifecycle.json
            --run-id
            084cc5a1-3cbe-4fef-bb5c-43559a1c13a0
            --backend
            yugabytedb_ysql
            --benchmark
            ycsb
            --backend-source-id
            yugabytedb_ysql_main
  )
  set_tests_properties(blackbox_cdc_replay_yugabytedb_fixture PROPERTIES
    PASS_REGULAR_EXPRESSION "\"completed_transactions\": 5"
  )
  add_test(
    NAME blackbox_tpcc_cdc_replay_contract
    COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_tpcc_cdc_replay.py
            --replay $<TARGET_FILE:blackbox_cdc_replay>
            --fixture
            ${CMAKE_CURRENT_SOURCE_DIR}/../protocol/fixtures/mariadb_tpcc_transactions.json
  )
  add_test(
    NAME blackbox_oracle_smallbank_codec_replay
    COMMAND blackbox_cdc_codec_replay
      --input ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/oracle_logminer_3_6_1_smallbank.json
      --backend oracle
      --benchmark smallbank
      --backend-source-id oracle_blackbox_main
  )
  set_tests_properties(blackbox_oracle_smallbank_codec_replay PROPERTIES
    PASS_REGULAR_EXPRESSION "\"completed_transactions\": 2"
  )
endif()
