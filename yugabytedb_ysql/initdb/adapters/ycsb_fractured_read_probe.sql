\set ON_ERROR_STOP on
SET search_path TO benchbase;

CREATE TABLE usertable (
  ycsb_key INTEGER PRIMARY KEY,
  field1 VARCHAR(100),
  field2 VARCHAR(100),
  field3 VARCHAR(100),
  field4 VARCHAR(100),
  field5 VARCHAR(100),
  field6 VARCHAR(100),
  field7 VARCHAR(100),
  field8 VARCHAR(100),
  field9 VARCHAR(100),
  field10 VARCHAR(100)
) SPLIT INTO 2 TABLETS;
