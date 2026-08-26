CREATE TABLE benchbase.accounts (
    custid BIGINT NOT NULL,
    name VARCHAR(64) COLLATE "C" NOT NULL,
    PRIMARY KEY (custid),
    CONSTRAINT accounts_name_unique UNIQUE (name)
) SPLIT INTO 2 TABLETS;

CREATE TABLE benchbase.savings (
    custid BIGINT NOT NULL,
    bal NUMERIC(18,2) NOT NULL,
    PRIMARY KEY (custid),
    CONSTRAINT savings_account_fk FOREIGN KEY (custid) REFERENCES benchbase.accounts(custid)
) SPLIT INTO 2 TABLETS;

CREATE TABLE benchbase.checking (
    custid BIGINT NOT NULL,
    bal NUMERIC(18,2) NOT NULL,
    PRIMARY KEY (custid),
    CONSTRAINT checking_account_fk FOREIGN KEY (custid) REFERENCES benchbase.accounts(custid)
) SPLIT INTO 2 TABLETS;
