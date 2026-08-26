set define on verify off echo off feedback on

CREATE USER blackbox_owner IDENTIFIED BY "&owner_password"
  DEFAULT TABLESPACE users QUOTA UNLIMITED ON users;
CREATE USER blackbox_application IDENTIFIED BY "&application_password";
GRANT CREATE SESSION TO blackbox_application;

CREATE OR REPLACE TRIGGER blackbox_application.default_schema_after_logon
AFTER LOGON ON blackbox_application.SCHEMA
BEGIN
  EXECUTE IMMEDIATE 'ALTER SESSION SET CURRENT_SCHEMA=BLACKBOX_OWNER';
  EXECUTE IMMEDIATE 'ALTER SESSION SET TIME_ZONE=''+00:00''';
  EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS=''.,''';
  EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_LANGUAGE=''AMERICAN''';
  EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_COMP=BINARY';
  EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_SORT=BINARY';
END;
/

ALTER USER c##blackbox_logminer QUOTA UNLIMITED ON users;
CREATE TABLE c##blackbox_logminer.log_mining_flush (
  last_scn NUMBER(19,0)
);

CREATE TABLE blackbox_owner.checker_run_fence (
  run_id CHAR(36 CHAR) NOT NULL,
  phase VARCHAR2(32 CHAR) NOT NULL,
  nonce CHAR(36 CHAR) NOT NULL,
  CONSTRAINT checker_run_fence_pk PRIMARY KEY (run_id, phase)
);

CREATE TABLE blackbox_owner.session_txn (
  session_id VARCHAR2(64 CHAR) NOT NULL,
  txn_id NUMBER(19,0) NOT NULL,
  CONSTRAINT session_txn_pk PRIMARY KEY (session_id, txn_id)
);

ALTER TABLE blackbox_owner.checker_run_fence
  ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE blackbox_owner.session_txn
  ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

GRANT SELECT, INSERT ON blackbox_owner.checker_run_fence TO blackbox_application;
GRANT SELECT, INSERT ON blackbox_owner.session_txn TO blackbox_application;
GRANT SELECT ON blackbox_owner.checker_run_fence TO c##blackbox_logminer;
GRANT SELECT ON blackbox_owner.session_txn TO c##blackbox_logminer;
