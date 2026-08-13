-- Placeholder workload schema: a bank of integer registers, the usual
-- testbed for SI anomalies (write skew needs two rows under one invariant).
-- Replace with your own workload schema; nothing else in the stack depends
-- on these table definitions.
USE sitest;

CREATE TABLE registers (
  id  INT    NOT NULL PRIMARY KEY,
  val BIGINT NOT NULL DEFAULT 0
) ENGINE=InnoDB;

INSERT INTO registers (id, val) VALUES (1, 0), (2, 0), (3, 0), (4, 0);
