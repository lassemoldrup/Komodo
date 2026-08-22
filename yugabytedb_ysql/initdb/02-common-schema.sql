\set ON_ERROR_STOP on
SET search_path TO benchbase;

CREATE TABLE checker_run_fence (
  run_id CHAR(36) NOT NULL,
  phase VARCHAR(32) NOT NULL,
  nonce CHAR(36) NOT NULL,
  PRIMARY KEY (run_id, phase)
) SPLIT INTO 1 TABLETS;

CREATE TABLE session_txn (
  session_id VARCHAR(64) PRIMARY KEY,
  txn_id BIGINT NOT NULL
) SPLIT INTO 1 TABLETS;

ALTER TABLE checker_run_fence REPLICA IDENTITY FULL;
ALTER TABLE session_txn REPLICA IDENTITY FULL;
