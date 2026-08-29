USE benchbase;

CREATE TABLE usertable (
  ycsb_key INTEGER NOT NULL,
  field1 VARCHAR(100),
  field2 VARCHAR(100),
  field3 VARCHAR(100),
  field4 VARCHAR(100),
  field5 VARCHAR(100),
  field6 VARCHAR(100),
  field7 VARCHAR(100),
  field8 VARCHAR(100),
  field9 VARCHAR(100),
  field10 VARCHAR(100),
  PRIMARY KEY (ycsb_key)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin
  PARTITION BY HASH(ycsb_key) PARTITIONS 4;

CREATE TABLE session_txn (
  session_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  txn_id BIGINT NOT NULL,
  PRIMARY KEY (session_id, txn_id)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE checker_run_fence (
  run_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  phase VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  nonce CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  PRIMARY KEY (run_id, phase)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
