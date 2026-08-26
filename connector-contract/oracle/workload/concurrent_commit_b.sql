whenever sqlerror exit failure rollback
ALTER SESSION SET CURRENT_SCHEMA=BLACKBOX_OWNER;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
INSERT INTO oracle_capability_rows(id,amount,fixed_text,variable_text)
VALUES(21,21.00,'B','commit-b');
INSERT INTO session_txn(session_id,txn_id)
VALUES('oracle/capability/concurrent-b',1);
COMMIT;
exit
