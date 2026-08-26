whenever sqlerror exit failure rollback
ALTER SESSION SET CURRENT_SCHEMA=BLACKBOX_OWNER;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
INSERT INTO oracle_capability_rows(id,amount,fixed_text,variable_text)
VALUES(20,20.00,'A','commit-a');
INSERT INTO session_txn(session_id,txn_id)
VALUES('oracle/capability/concurrent-a',1);
host sleep 3
COMMIT;
exit
