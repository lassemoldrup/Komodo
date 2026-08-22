\set ON_ERROR_STOP on

SET search_path TO benchbase;

CREATE TABLE usertable (
    ycsb_key INTEGER PRIMARY KEY,
    field1 VARCHAR(100),
    field2 VARCHAR(100),
    field3 VARCHAR(100),
    field4 VARCHAR(100),
    field5 VARCHAR(100),
    field6 VARCHAR(100),
    field7 VARCHAR(100),
    field8 VARCHAR(100),
    field9 VARCHAR(100),
    field10 VARCHAR(100)
) SPLIT INTO 2 TABLETS;

CREATE TABLE checker_run_fence (
    run_id VARCHAR(36) NOT NULL,
    phase VARCHAR(32) NOT NULL,
    nonce VARCHAR(36) NOT NULL,
    PRIMARY KEY (run_id, phase)
) SPLIT INTO 1 TABLETS;

CREATE TABLE session_txn (
    session_id VARCHAR(64) PRIMARY KEY,
    txn_id BIGINT NOT NULL
) SPLIT INTO 1 TABLETS;

ALTER TABLE usertable REPLICA IDENTITY FULL;
ALTER TABLE checker_run_fence REPLICA IDENTITY FULL;
ALTER TABLE session_txn REPLICA IDENTITY FULL;

CREATE PUBLICATION connector_contract_publication FOR TABLE
    benchbase.usertable,
    benchbase.checker_run_fence,
    benchbase.session_txn;

GRANT USAGE ON SCHEMA benchbase TO connector_contract_application;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA benchbase
    TO connector_contract_application;

GRANT USAGE ON SCHEMA benchbase TO connector_contract_replication;
GRANT SELECT ON ALL TABLES IN SCHEMA benchbase
    TO connector_contract_replication;
