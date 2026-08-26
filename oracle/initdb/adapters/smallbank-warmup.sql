set heading off feedback off pagesize 0
whenever sqlerror continue
ALTER SESSION SET CURRENT_SCHEMA=BLACKBOX_OWNER;

SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
INSERT INTO accounts(custid,name) VALUES(-1,'__oracle_warmup__');
INSERT INTO savings(custid,bal) VALUES(-1,0.00);
INSERT INTO checking(custid,bal) VALUES(-1,0.00);
INSERT INTO session_txn(session_id,txn_id) VALUES('__oracle_warmup__',1);
INSERT INTO checker_run_fence(run_id,phase,nonce)
VALUES('00000000-0000-0000-0000-000000000000','WARMUP','00000000-0000-0000-0000-000000000000');
DELETE FROM checking WHERE custid=-1;
DELETE FROM savings WHERE custid=-1;
DELETE FROM accounts WHERE custid=-1;
COMMIT;

SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
INSERT INTO accounts(custid,name) VALUES(-1,'__oracle_warmup__');
INSERT INTO savings(custid,bal) VALUES(-1,0.00);
INSERT INTO checking(custid,bal) VALUES(-1,0.00);
INSERT INTO session_txn(session_id,txn_id) VALUES('__oracle_warmup__',1);
INSERT INTO checker_run_fence(run_id,phase,nonce)
VALUES('00000000-0000-0000-0000-000000000000','WARMUP','00000000-0000-0000-0000-000000000000');
DELETE FROM checking WHERE custid=-1;
DELETE FROM savings WHERE custid=-1;
DELETE FROM accounts WHERE custid=-1;
COMMIT;

SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
INSERT INTO accounts(custid,name) VALUES(-1,'__oracle_warmup__');
INSERT INTO savings(custid,bal) VALUES(-1,0.00);
INSERT INTO checking(custid,bal) VALUES(-1,0.00);
INSERT INTO session_txn(session_id,txn_id) VALUES('__oracle_warmup__',1);
INSERT INTO checker_run_fence(run_id,phase,nonce)
VALUES('00000000-0000-0000-0000-000000000000','WARMUP','00000000-0000-0000-0000-000000000000');
DELETE FROM checking WHERE custid=-1;
DELETE FROM savings WHERE custid=-1;
DELETE FROM accounts WHERE custid=-1;
COMMIT;

whenever sqlerror exit failure rollback
SELECT CASE WHEN
  (SELECT COUNT(*) FROM accounts) +
  (SELECT COUNT(*) FROM savings) +
  (SELECT COUNT(*) FROM checking) +
  (SELECT COUNT(*) FROM session_txn) +
  (SELECT COUNT(*) FROM checker_run_fence) = 0
  THEN 1 ELSE 0 END FROM dual;
exit
