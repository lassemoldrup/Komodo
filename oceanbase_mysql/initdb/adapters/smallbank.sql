USE benchbase;

CREATE TABLE accounts (
  custid BIGINT NOT NULL,
  name VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  PRIMARY KEY (custid),
  UNIQUE KEY accounts_name_unique (name)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE savings (
  custid BIGINT NOT NULL,
  bal DECIMAL(18,2) NOT NULL,
  PRIMARY KEY (custid),
  CONSTRAINT savings_account_fk FOREIGN KEY (custid) REFERENCES accounts(custid)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE checking (
  custid BIGINT NOT NULL,
  bal DECIMAL(18,2) NOT NULL,
  PRIMARY KEY (custid),
  CONSTRAINT checking_account_fk FOREIGN KEY (custid) REFERENCES accounts(custid)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE session_txn (
  session_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  txn_id BIGINT NOT NULL,
  PRIMARY KEY (session_id, txn_id)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE checker_run_fence (
  run_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  phase VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  nonce CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  PRIMARY KEY (run_id, phase)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
