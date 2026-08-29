SET NAMES utf8mb4 COLLATE utf8mb4_bin;
SET time_zone = '+00:00';
SET GLOBAL time_zone = '+00:00';
SET GLOBAL transaction_isolation = 'REPEATABLE-READ';
SET GLOBAL collation_server = 'utf8mb4_bin';
SET GLOBAL sql_mode = 'STRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

CREATE DATABASE benchbase
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_bin;

CREATE USER blackbox_setup IDENTIFIED BY 'OceanBaseSetup_2026!';
CREATE USER blackbox_application IDENTIFIED BY 'OceanBaseApplication_2026!';
CREATE USER blackbox_connector IDENTIFIED BY 'OceanBaseConnector_2026!';

GRANT ALL PRIVILEGES ON benchbase.* TO blackbox_setup;
GRANT SELECT, INSERT, UPDATE, DELETE ON benchbase.* TO blackbox_application;
GRANT SELECT ON benchbase.* TO blackbox_connector;
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO blackbox_connector;
