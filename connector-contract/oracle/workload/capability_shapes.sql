whenever sqlerror exit failure rollback
ALTER SESSION SET CURRENT_SCHEMA=BLACKBOX_OWNER;

SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
INSERT INTO oracle_capability_rows(id,amount,fixed_text,variable_text,calendar_date,event_time)
VALUES(1,12.30,'A','',DATE '2026-08-25',TIMESTAMP '2026-08-25 16:41:00.123456');
INSERT INTO session_txn(session_id,txn_id) VALUES('oracle/capability/main',1);
COMMIT;

SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
UPDATE oracle_capability_rows SET amount=22.40, variable_text='temporary' WHERE id=1;
UPDATE oracle_capability_rows SET amount=12.30, variable_text=NULL WHERE id=1;
INSERT INTO session_txn(session_id,txn_id) VALUES('oracle/capability/main',2);
COMMIT;

SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
INSERT INTO oracle_capability_rows(id,amount,fixed_text,variable_text,calendar_date,event_time)
VALUES(2,NULL,'BC','insert-delete',NULL,NULL);
DELETE FROM oracle_capability_rows WHERE id=2;
INSERT INTO session_txn(session_id,txn_id) VALUES('oracle/capability/main',3);
COMMIT;

SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
UPDATE oracle_capability_rows SET id=101 WHERE id=1;
INSERT INTO session_txn(session_id,txn_id) VALUES('oracle/capability/main',4);
COMMIT;

SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
DELETE FROM oracle_capability_rows WHERE id=101;
INSERT INTO session_txn(session_id,txn_id) VALUES('oracle/capability/main',5);
COMMIT;
exit
