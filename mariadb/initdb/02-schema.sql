-- Create every modeled table before Debezium starts, but deliberately leave
-- application state empty. The BenchBase loader is captured as the bootstrap
-- CDC prefix; pre-connector seed rows would make that prefix unverifiable.
USE benchbase;

CREATE TABLE usertable (
  ycsb_key INT NOT NULL PRIMARY KEY,
  field1   VARCHAR(100) NULL,
  field2   VARCHAR(100) NULL,
  field3   VARCHAR(100) NULL,
  field4   VARCHAR(100) NULL,
  field5   VARCHAR(100) NULL,
  field6   VARCHAR(100) NULL,
  field7   VARCHAR(100) NULL,
  field8   VARCHAR(100) NULL,
  field9   VARCHAR(100) NULL,
  field10  VARCHAR(100) NULL
) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

-- Each lifecycle fence is inserted and committed in its own transaction by
-- scripts/e2e.sh. The composite primary key makes a repeated phase explicit.
CREATE TABLE checker_run_fence (
  run_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  phase  VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  nonce  CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  PRIMARY KEY (run_id, phase)
) ENGINE=InnoDB;

-- One row per workload session. Every physical attempt writes its fresh,
-- session-local txn_id here as the final statement before commit. The table
-- deliberately has no run_id: a fresh stack plus the fenced one-run lifecycle
-- supplies the run scope.
CREATE TABLE session_txn (
  session_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  txn_id     BIGINT NOT NULL,
  PRIMARY KEY (session_id)
) ENGINE=InnoDB;
