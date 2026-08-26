CREATE TABLE blackbox_owner.oracle_capability_rows (
  id NUMBER(19,0) NOT NULL,
  amount NUMBER(18,2),
  fixed_text CHAR(4 CHAR),
  variable_text VARCHAR2(32 CHAR),
  calendar_date DATE,
  event_time TIMESTAMP(6),
  CONSTRAINT oracle_capability_rows_pk PRIMARY KEY (id)
);

ALTER TABLE blackbox_owner.oracle_capability_rows
  ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
GRANT SELECT, INSERT, UPDATE, DELETE ON blackbox_owner.oracle_capability_rows
  TO blackbox_application;
GRANT SELECT ON blackbox_owner.oracle_capability_rows TO c##blackbox_logminer;
