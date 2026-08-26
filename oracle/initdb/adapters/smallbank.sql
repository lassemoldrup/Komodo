CREATE TABLE blackbox_owner.accounts (
  custid NUMBER(19,0) NOT NULL,
  name VARCHAR2(64 CHAR) NOT NULL,
  CONSTRAINT accounts_pk PRIMARY KEY (custid),
  CONSTRAINT accounts_name_unique UNIQUE (name)
);

CREATE TABLE blackbox_owner.savings (
  custid NUMBER(19,0) NOT NULL,
  bal NUMBER(18,2) NOT NULL,
  CONSTRAINT savings_pk PRIMARY KEY (custid),
  CONSTRAINT savings_account_fk FOREIGN KEY (custid)
    REFERENCES blackbox_owner.accounts(custid)
);

CREATE TABLE blackbox_owner.checking (
  custid NUMBER(19,0) NOT NULL,
  bal NUMBER(18,2) NOT NULL,
  CONSTRAINT checking_pk PRIMARY KEY (custid),
  CONSTRAINT checking_account_fk FOREIGN KEY (custid)
    REFERENCES blackbox_owner.accounts(custid)
);

ALTER TABLE blackbox_owner.accounts ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE blackbox_owner.savings ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE blackbox_owner.checking ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

GRANT SELECT, INSERT, UPDATE, DELETE ON blackbox_owner.accounts TO blackbox_application;
GRANT SELECT, INSERT, UPDATE, DELETE ON blackbox_owner.savings TO blackbox_application;
GRANT SELECT, INSERT, UPDATE, DELETE ON blackbox_owner.checking TO blackbox_application;
GRANT SELECT ON blackbox_owner.accounts TO c##blackbox_logminer;
GRANT SELECT ON blackbox_owner.savings TO c##blackbox_logminer;
GRANT SELECT ON blackbox_owner.checking TO c##blackbox_logminer;
