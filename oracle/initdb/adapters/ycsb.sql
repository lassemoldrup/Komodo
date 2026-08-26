CREATE TABLE blackbox_owner.usertable (
  ycsb_key NUMBER(10,0) NOT NULL,
  field1 VARCHAR2(100 CHAR),
  field2 VARCHAR2(100 CHAR),
  field3 VARCHAR2(100 CHAR),
  field4 VARCHAR2(100 CHAR),
  field5 VARCHAR2(100 CHAR),
  field6 VARCHAR2(100 CHAR),
  field7 VARCHAR2(100 CHAR),
  field8 VARCHAR2(100 CHAR),
  field9 VARCHAR2(100 CHAR),
  field10 VARCHAR2(100 CHAR),
  CONSTRAINT usertable_pk PRIMARY KEY (ycsb_key)
);

ALTER TABLE blackbox_owner.usertable ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
GRANT SELECT, INSERT, UPDATE, DELETE ON blackbox_owner.usertable TO blackbox_application;
GRANT SELECT ON blackbox_owner.usertable TO c##blackbox_logminer;
