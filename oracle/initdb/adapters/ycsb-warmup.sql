set heading off feedback off pagesize 0
whenever sqlerror continue
ALTER SESSION SET CURRENT_SCHEMA=BLACKBOX_OWNER;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
INSERT INTO session_txn(session_id,txn_id) VALUES('__oracle_warmup__',1);
INSERT INTO checker_run_fence(run_id,phase,nonce)
VALUES('00000000-0000-0000-0000-000000000000','WARMUP','00000000-0000-0000-0000-000000000000');
COMMIT;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
INSERT INTO session_txn(session_id,txn_id) VALUES('__oracle_warmup__',1);
INSERT INTO checker_run_fence(run_id,phase,nonce)
VALUES('00000000-0000-0000-0000-000000000000','WARMUP','00000000-0000-0000-0000-000000000000');
COMMIT;
whenever sqlerror exit failure rollback
exit
