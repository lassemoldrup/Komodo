-- Selected adapter schema: YCSB. Application state remains empty until the
-- captured loader runs after CDC starts.
USE benchbase;

CREATE TABLE usertable (
  ycsb_key INT NOT NULL PRIMARY KEY,
  field1   VARCHAR(100) NULL,
  field2   VARCHAR(100) NULL,
  field3   VARCHAR(100) NULL,
  field4   VARCHAR(100) NULL,
  field5   VARCHAR(100) NULL,
  field6   VARCHAR(100) NULL,
  field7   VARCHAR(100) NULL,
  field8   VARCHAR(100) NULL,
  field9   VARCHAR(100) NULL,
  field10  VARCHAR(100) NULL
) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
