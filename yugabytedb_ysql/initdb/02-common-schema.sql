\set ON_ERROR_STOP on
SET search_path TO benchbase;

CREATE TABLE checker_run_fence (
  run_id CHAR(36) NOT NULL,
  phase VARCHAR(32) NOT NULL,
  nonce CHAR(36) NOT NULL,
  PRIMARY KEY (run_id, phase)
) SPLIT INTO 1 TABLETS;

CREATE TABLE session_txn (
  session_id VARCHAR(64) NOT NULL,
  txn_id BIGINT NOT NULL,
  PRIMARY KEY (session_id, txn_id)
) SPLIT INTO 1 TABLETS;

ALTER TABLE checker_run_fence REPLICA IDENTITY FULL;
ALTER TABLE session_txn REPLICA IDENTITY FULL;
