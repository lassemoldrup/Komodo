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
    session_id VARCHAR(64) NOT NULL,
    txn_id BIGINT NOT NULL,
    PRIMARY KEY (session_id, txn_id)
) SPLIT INTO 1 TABLETS;

-- Contract-only tables used to prove cross-table and four-tablet ordering.
-- They are not benchmark adapter assets and never enter an audited run.
CREATE TABLE contract_account_a (
    account_id INTEGER PRIMARY KEY,
    payload VARCHAR(100) NOT NULL
) SPLIT INTO 1 TABLETS;

CREATE TABLE contract_account_b (
    account_id INTEGER PRIMARY KEY,
    payload VARCHAR(100) NOT NULL
) SPLIT INTO 1 TABLETS;

CREATE TABLE contract_account_c (
    account_id INTEGER PRIMARY KEY,
    payload VARCHAR(100) NOT NULL
) SPLIT INTO 1 TABLETS;

CREATE TABLE contract_account_d (
    account_id INTEGER PRIMARY KEY,
    payload VARCHAR(100) NOT NULL
) SPLIT INTO 1 TABLETS;

-- Two exact tables from the frozen TPC-C logical contract. Their only purpose
-- here is the cross-table/tablet transaction-framing experiment; the complete
-- evidence-compatible YSQL TPC-C schema belongs to the later backend step.
CREATE TABLE warehouse (
    w_id INTEGER PRIMARY KEY,
    w_ytd DECIMAL(12, 2) NOT NULL,
    w_tax DECIMAL(4, 4) NOT NULL,
    w_name VARCHAR(10) NOT NULL,
    w_street_1 VARCHAR(20) NOT NULL,
    w_street_2 VARCHAR(20) NOT NULL,
    w_city VARCHAR(20) NOT NULL,
    w_state CHAR(2) NOT NULL,
    w_zip CHAR(9) NOT NULL
) SPLIT INTO 1 TABLETS;

CREATE TABLE district (
    d_w_id INTEGER NOT NULL,
    d_id INTEGER NOT NULL,
    d_ytd DECIMAL(12, 2) NOT NULL,
    d_tax DECIMAL(4, 4) NOT NULL,
    d_next_o_id INTEGER NOT NULL,
    d_name VARCHAR(10) NOT NULL,
    d_street_1 VARCHAR(20) NOT NULL,
    d_street_2 VARCHAR(20) NOT NULL,
    d_city VARCHAR(20) NOT NULL,
    d_state CHAR(2) NOT NULL,
    d_zip CHAR(9) NOT NULL,
    PRIMARY KEY (d_w_id, d_id)
) SPLIT INTO 1 TABLETS;

ALTER TABLE usertable REPLICA IDENTITY FULL;
ALTER TABLE checker_run_fence REPLICA IDENTITY FULL;
ALTER TABLE session_txn REPLICA IDENTITY FULL;
ALTER TABLE contract_account_a REPLICA IDENTITY FULL;
ALTER TABLE contract_account_b REPLICA IDENTITY FULL;
ALTER TABLE contract_account_c REPLICA IDENTITY FULL;
ALTER TABLE contract_account_d REPLICA IDENTITY FULL;
ALTER TABLE warehouse REPLICA IDENTITY FULL;
ALTER TABLE district REPLICA IDENTITY FULL;

CREATE PUBLICATION connector_contract_publication FOR TABLE
    benchbase.usertable,
    benchbase.checker_run_fence,
    benchbase.session_txn,
    benchbase.contract_account_a,
    benchbase.contract_account_b,
    benchbase.contract_account_c,
    benchbase.contract_account_d,
    benchbase.warehouse,
    benchbase.district;

GRANT USAGE ON SCHEMA benchbase TO connector_contract_application;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA benchbase
    TO connector_contract_application;

GRANT USAGE ON SCHEMA benchbase TO connector_contract_replication;
GRANT SELECT ON ALL TABLES IN SCHEMA benchbase
    TO connector_contract_replication;
