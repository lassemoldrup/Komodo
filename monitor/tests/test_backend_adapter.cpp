#include "blackbox/backend_adapter.h"

#include "test_support.h"

#include <nlohmann/json.hpp>

#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>

#ifndef BLACKBOX_PROTOCOL_BACKEND_DIR
#error "BLACKBOX_PROTOCOL_BACKEND_DIR must name the protocol backend directory"
#endif

namespace {

using blackbox::BackendAdapterRegistry;
using blackbox::MonitorError;
using blackbox::MonitorErrorCode;
using Json = nlohmann::json;

[[nodiscard]] Json loadBackendContract() {
  const auto path =
      std::filesystem::path(BLACKBOX_PROTOCOL_BACKEND_DIR) / "mariadb.json";
  std::ifstream input(path, std::ios::binary);
  if (!input.good()) {
    throw std::runtime_error("cannot open shared backend contract: " +
                             path.string());
  }
  Json result;
  input >> result;
  return result;
}

void checkSharedMariaDbContract() {
  const auto contract = loadBackendContract();
  const auto &adapter = BackendAdapterRegistry::require("mariadb");
  BLACKBOX_CHECK(contract.at("backend").get<std::string>() == adapter.name());

  const auto server = adapter.serverDefaults();
  const auto &expected_server = contract.at("server_defaults");
  BLACKBOX_CHECK(server.character_set ==
                 expected_server.at("character_set").get<std::string>());
  BLACKBOX_CHECK(server.collation ==
                 expected_server.at("collation").get<std::string>());
  BLACKBOX_CHECK(server.time_zone ==
                 expected_server.at("time_zone").get<std::string>());
  BLACKBOX_CHECK(server.sql_mode ==
                 expected_server.at("sql_mode").get<std::string>());
  BLACKBOX_CHECK(
      server.transaction_isolation ==
      expected_server.at("transaction_isolation").get<std::string>());
  BLACKBOX_CHECK(server.snapshot_isolation ==
                 expected_server.at("snapshot_isolation").get<bool>());

  const auto session = adapter.sessionRequirements();
  const auto &expected_session = contract.at("session_requirements");
  BLACKBOX_CHECK(session.set_names ==
                 expected_session.at("set_names").get<std::string>());
  BLACKBOX_CHECK(
      session.character_set_client ==
      expected_session.at("character_set_client").get<std::string>());
  BLACKBOX_CHECK(
      session.character_set_connection ==
      expected_session.at("character_set_connection").get<std::string>());
  BLACKBOX_CHECK(
      session.character_set_results ==
      expected_session.at("character_set_results").get<std::string>());
  BLACKBOX_CHECK(
      session.collation_connection ==
      expected_session.at("collation_connection").get<std::string>());
  BLACKBOX_CHECK(session.time_zone ==
                 expected_session.at("time_zone").get<std::string>());
  BLACKBOX_CHECK(session.sql_mode ==
                 expected_session.at("sql_mode").get<std::string>());
  BLACKBOX_CHECK(
      session.transaction_isolation ==
      expected_session.at("transaction_isolation").get<std::string>());
  BLACKBOX_CHECK(session.snapshot_isolation ==
                 expected_session.at("snapshot_isolation").get<bool>());

  const auto database = adapter.databaseDefaults();
  const auto &expected_database = contract.at("database_defaults");
  BLACKBOX_CHECK(database.character_set ==
                 expected_database.at("character_set").get<std::string>());
  BLACKBOX_CHECK(database.collation ==
                 expected_database.at("collation").get<std::string>());

  const auto application = adapter.applicationTableRequirements();
  const auto &expected_application =
      contract.at("application_table_requirements");
  BLACKBOX_CHECK(application.engine ==
                 expected_application.at("engine").get<std::string>());
  BLACKBOX_CHECK(application.character_set ==
                 expected_application.at("character_set").get<std::string>());
  BLACKBOX_CHECK(application.collation ==
                 expected_application.at("collation").get<std::string>());
  BLACKBOX_CHECK(
      application.char_read_semantics ==
      expected_application.at("char_read_semantics").get<std::string>());
}

void checkRegistry() {
  BLACKBOX_CHECK(
      blackbox::toString(MonitorErrorCode::SessionContractMismatch) ==
      "SESSION_CONTRACT_MISMATCH");
  BLACKBOX_CHECK(BackendAdapterRegistry::adapters().size() == 1U);
  BLACKBOX_CHECK(BackendAdapterRegistry::find("mariadb") != nullptr);
  BLACKBOX_CHECK(BackendAdapterRegistry::find("unknown") == nullptr);
  try {
    (void)BackendAdapterRegistry::require("unknown");
    BLACKBOX_CHECK(false);
  } catch (const MonitorError &error) {
    BLACKBOX_CHECK(error.code() == MonitorErrorCode::ConfigInvalid);
  }
}

} // namespace

int runBackendAdapterTests() {
  checkSharedMariaDbContract();
  checkRegistry();
  return 0;
}
