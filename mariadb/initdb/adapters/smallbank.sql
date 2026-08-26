CREATE TABLE accounts (
    custid BIGINT NOT NULL,
    name VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    PRIMARY KEY (custid),
    UNIQUE KEY accounts_name_unique (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE savings (
    custid BIGINT NOT NULL,
    bal DECIMAL(18,2) NOT NULL,
    PRIMARY KEY (custid),
    CONSTRAINT savings_account_fk FOREIGN KEY (custid) REFERENCES accounts(custid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE checking (
    custid BIGINT NOT NULL,
    bal DECIMAL(18,2) NOT NULL,
    PRIMARY KEY (custid),
    CONSTRAINT checking_account_fk FOREIGN KEY (custid) REFERENCES accounts(custid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
