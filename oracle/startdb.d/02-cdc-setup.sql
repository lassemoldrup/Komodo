-- ---------------------------------------------------------------------------
-- Oracle CDC prerequisites + workload schema. Idempotent: re-running raises
-- "already exists" errors (ORA-01920 / ORA-00955 / ORA-32588) that
-- 01-cdc-setup.sh deliberately tolerates.
-- ---------------------------------------------------------------------------
set echo off feedback on

-- Debezium LogMiner needs force logging plus at least minimal supplemental
-- logging at the database level.
ALTER DATABASE FORCE LOGGING;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

-- ---------------------------------------------------------------------------
-- LogMiner user. Must be a CDB common user (c## prefix) with CONTAINER=ALL,
-- because the connector mines redo at the CDB level while reading table
-- metadata from the PDB.
-- ---------------------------------------------------------------------------
CREATE USER c##dbzuser IDENTIFIED BY dbz DEFAULT TABLESPACE users
  QUOTA UNLIMITED ON users CONTAINER=ALL;

GRANT CREATE SESSION, SET CONTAINER, SELECT ANY TABLE, SELECT ANY TRANSACTION,
      SELECT_CATALOG_ROLE, EXECUTE_CATALOG_ROLE, LOGMINING, FLASHBACK ANY TABLE,
      CREATE TABLE, LOCK ANY TABLE, CREATE SEQUENCE TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR   TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR_D TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$DATABASE            TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOG                 TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOG_HISTORY         TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_LOGS         TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_CONTENTS     TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_PARAMETERS   TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGFILE             TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVED_LOG        TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$TRANSACTION         TO c##dbzuser CONTAINER=ALL;

-- ---------------------------------------------------------------------------
-- Workload schema, in the PDB.
-- ---------------------------------------------------------------------------
ALTER SESSION SET CONTAINER = FREEPDB1;

CREATE USER sitest IDENTIFIED BY sitestpw DEFAULT TABLESPACE users
  QUOTA UNLIMITED ON users;
GRANT CREATE SESSION, CREATE TABLE, CREATE TRIGGER TO sitest;

CREATE TABLE sitest.registers (
  id  NUMBER(10) PRIMARY KEY,
  val NUMBER(19) DEFAULT 0 NOT NULL
);

-- Transaction marker. Identical role on every backend: one row per client
-- session, upserted as the LAST statement of each transaction, so the marker
-- travels inside that transaction's own change set and identifies it in the
-- CDC stream. Starts empty; sessions create their own row on first use.
CREATE TABLE sitest.session_txn (
  session_id VARCHAR2(64) PRIMARY KEY,
  txn_id     NUMBER(19) DEFAULT 0 NOT NULL
);

INSERT INTO sitest.registers (id, val) VALUES (1, 0);
INSERT INTO sitest.registers (id, val) VALUES (2, 0);
INSERT INTO sitest.registers (id, val) VALUES (3, 0);
INSERT INTO sitest.registers (id, val) VALUES (4, 0);
COMMIT;

-- (ALL) COLUMNS is the Oracle equivalent of REPLICA IDENTITY FULL: without it
-- the monitor gets only changed columns plus the key, not a full before-image.
ALTER TABLE sitest.registers   ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE sitest.session_txn ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- ---------------------------------------------------------------------------
-- Default isolation: Oracle's SERIALIZABLE *is* Snapshot Isolation, and the
-- server default is READ COMMITTED. Oracle has no instance parameter for the
-- default isolation level, so a logon trigger supplies it — every workload
-- session then gets SI without the client setting anything.
--
-- Scoped to SITEST only: the LogMiner session must keep default behaviour.
-- ---------------------------------------------------------------------------
-- `ON sitest.SCHEMA`, not `ON SCHEMA`: the unqualified form binds to the
-- creating session's schema (SYS here) and fails with ORA-30510.
CREATE OR REPLACE TRIGGER sitest.set_si_isolation
  AFTER LOGON ON sitest.SCHEMA
BEGIN
  EXECUTE IMMEDIATE 'ALTER SESSION SET ISOLATION_LEVEL = SERIALIZABLE';
END;
/

ALTER TRIGGER sitest.set_si_isolation ENABLE;

exit
