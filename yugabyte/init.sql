-- ---------------------------------------------------------------------------
-- YugabyteDB workload schema + transaction-marker table.
--
-- ORDER MATTERS. Everything here runs BEFORE the replication slot is created
-- (see init.sh). DDL against a table that already has an active slot
-- permanently breaks before-image computation for that tablet, and the
-- connector then retries forever instead of failing.
-- ---------------------------------------------------------------------------

-- Placeholder workload schema: a bank of integer registers, the usual
-- write-skew testbed. Replace with your own workload schema.
CREATE TABLE IF NOT EXISTS registers (
  id  int    PRIMARY KEY,
  val bigint NOT NULL DEFAULT 0
);

-- ---------------------------------------------------------------------------
-- Transaction marker.
--
-- One row per client session, upserted in place as the LAST statement of each
-- transaction, so the CDC stream carries (session_id, txn_id) inside that
-- transaction's own change set. That is the join key between the client's
-- read log and the stream.
--
-- Why a fixed row per session rather than an inserted row per transaction:
--   * the table never grows
--   * sessions only ever touch their OWN row, so the marker introduces no
--     cross-session write conflicts under SI
--
-- Upsert rather than UPDATE means sessions need no provisioning: a session id
-- that has never been seen inserts its row on first use (op=c, before=null)
-- and updates it thereafter (op=u, carrying the previous txn_id).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS session_txn (
  session_id text   PRIMARY KEY,
  txn_id     bigint NOT NULL DEFAULT 0
);

-- FULL replica identity: the monitor needs the before-image of every write,
-- and for session_txn it also yields the previous txn_id as a free
-- consistency check that no transaction was missed on that session.
ALTER TABLE registers   REPLICA IDENTITY FULL;
ALTER TABLE session_txn REPLICA IDENTITY FULL;

CREATE PUBLICATION cdc_pub FOR ALL TABLES;

-- Seed the workload rows.
INSERT INTO registers
SELECT g, 0 FROM generate_series(1, 4) g
ON CONFLICT (id) DO NOTHING;

-- No session rows are pre-created: the client's upsert creates each session's
-- row on first use. session_txn deliberately starts empty.
