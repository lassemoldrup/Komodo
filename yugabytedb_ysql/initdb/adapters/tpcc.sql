\set ON_ERROR_STOP on
SET search_path TO benchbase;

-- Selected adapter schema: BenchBase TPC-C with an evidence-only history key.
-- Column order, logical types, nullability, and keys map the shared adapter
-- contract exactly: NUMERIC keeps the declared fixed scales, REAL carries the
-- canonical float32, TIMESTAMPTZ(0) freezes UTC whole-second timestamps, and
-- CHAR keeps its exact declared width. Application state remains empty until
-- the captured loader runs after CDC.

CREATE TABLE warehouse (
  w_id       INT           NOT NULL,
  w_ytd      NUMERIC(12,2) NOT NULL,
  w_tax      NUMERIC(4,4)  NOT NULL,
  w_name     VARCHAR(10)   NOT NULL,
  w_street_1 VARCHAR(20)   NOT NULL,
  w_street_2 VARCHAR(20)   NOT NULL,
  w_city     VARCHAR(20)   NOT NULL,
  w_state    CHAR(2)       NOT NULL,
  w_zip      CHAR(9)       NOT NULL,
  PRIMARY KEY (w_id)
) SPLIT INTO 1 TABLETS;

CREATE TABLE item (
  i_id    INT          NOT NULL,
  i_name  VARCHAR(24)  NOT NULL,
  i_price NUMERIC(5,2) NOT NULL,
  i_data  VARCHAR(50)  NOT NULL,
  i_im_id INT          NOT NULL,
  PRIMARY KEY (i_id)
) SPLIT INTO 2 TABLETS;

CREATE TABLE stock (
  s_w_id       INT          NOT NULL,
  s_i_id       INT          NOT NULL,
  s_quantity   INT          NOT NULL,
  s_ytd        NUMERIC(8,2) NOT NULL,
  s_order_cnt  INT          NOT NULL,
  s_remote_cnt INT          NOT NULL,
  s_data       VARCHAR(50)  NOT NULL,
  s_dist_01    CHAR(24)     NOT NULL,
  s_dist_02    CHAR(24)     NOT NULL,
  s_dist_03    CHAR(24)     NOT NULL,
  s_dist_04    CHAR(24)     NOT NULL,
  s_dist_05    CHAR(24)     NOT NULL,
  s_dist_06    CHAR(24)     NOT NULL,
  s_dist_07    CHAR(24)     NOT NULL,
  s_dist_08    CHAR(24)     NOT NULL,
  s_dist_09    CHAR(24)     NOT NULL,
  s_dist_10    CHAR(24)     NOT NULL,
  FOREIGN KEY (s_w_id) REFERENCES warehouse (w_id) ON DELETE CASCADE,
  FOREIGN KEY (s_i_id) REFERENCES item (i_id) ON DELETE CASCADE,
  PRIMARY KEY (s_w_id, s_i_id)
) SPLIT INTO 2 TABLETS;

CREATE TABLE district (
  d_w_id      INT           NOT NULL,
  d_id        INT           NOT NULL,
  d_ytd       NUMERIC(12,2) NOT NULL,
  d_tax       NUMERIC(4,4)  NOT NULL,
  d_next_o_id INT           NOT NULL,
  d_name      VARCHAR(10)   NOT NULL,
  d_street_1  VARCHAR(20)   NOT NULL,
  d_street_2  VARCHAR(20)   NOT NULL,
  d_city      VARCHAR(20)   NOT NULL,
  d_state     CHAR(2)       NOT NULL,
  d_zip       CHAR(9)       NOT NULL,
  FOREIGN KEY (d_w_id) REFERENCES warehouse (w_id) ON DELETE CASCADE,
  PRIMARY KEY (d_w_id, d_id)
) SPLIT INTO 1 TABLETS;

CREATE TABLE customer (
  c_w_id         INT           NOT NULL,
  c_d_id         INT           NOT NULL,
  c_id           INT           NOT NULL,
  c_discount     NUMERIC(4,4)  NOT NULL,
  c_credit       CHAR(2)       NOT NULL,
  c_last         VARCHAR(16)   NOT NULL,
  c_first        VARCHAR(16)   NOT NULL,
  c_credit_lim   NUMERIC(12,2) NOT NULL,
  c_balance      NUMERIC(12,2) NOT NULL,
  c_ytd_payment  REAL          NOT NULL,
  c_payment_cnt  INT           NOT NULL,
  c_delivery_cnt INT           NOT NULL,
  c_street_1     VARCHAR(20)   NOT NULL,
  c_street_2     VARCHAR(20)   NOT NULL,
  c_city         VARCHAR(20)   NOT NULL,
  c_state        CHAR(2)       NOT NULL,
  c_zip          CHAR(9)       NOT NULL,
  c_phone        CHAR(16)      NOT NULL,
  c_since        TIMESTAMPTZ(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  c_middle       CHAR(2)       NOT NULL,
  c_data         VARCHAR(500)  NOT NULL,
  FOREIGN KEY (c_w_id, c_d_id) REFERENCES district (d_w_id, d_id) ON DELETE CASCADE,
  PRIMARY KEY (c_w_id, c_d_id, c_id)
) SPLIT INTO 2 TABLETS;

CREATE TABLE history (
  h_id     BIGINT GENERATED ALWAYS AS IDENTITY,
  h_c_id   INT            NOT NULL,
  h_c_d_id INT            NOT NULL,
  h_c_w_id INT            NOT NULL,
  h_d_id   INT            NOT NULL,
  h_w_id   INT            NOT NULL,
  h_date   TIMESTAMPTZ(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  h_amount NUMERIC(6,2)   NOT NULL,
  h_data   VARCHAR(24)    NOT NULL,
  FOREIGN KEY (h_c_w_id, h_c_d_id, h_c_id)
    REFERENCES customer (c_w_id, c_d_id, c_id) ON DELETE CASCADE,
  FOREIGN KEY (h_w_id, h_d_id)
    REFERENCES district (d_w_id, d_id) ON DELETE CASCADE,
  PRIMARY KEY (h_id)
) SPLIT INTO 2 TABLETS;

CREATE TABLE oorder (
  o_w_id       INT            NOT NULL,
  o_d_id       INT            NOT NULL,
  o_id         INT            NOT NULL,
  o_c_id       INT            NOT NULL,
  o_carrier_id INT                     DEFAULT NULL,
  o_ol_cnt     INT            NOT NULL,
  o_all_local  INT            NOT NULL,
  o_entry_d    TIMESTAMPTZ(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  PRIMARY KEY (o_w_id, o_d_id, o_id),
  FOREIGN KEY (o_w_id, o_d_id, o_c_id)
    REFERENCES customer (c_w_id, c_d_id, c_id) ON DELETE CASCADE,
  UNIQUE (o_w_id, o_d_id, o_c_id, o_id)
) SPLIT INTO 2 TABLETS;

CREATE TABLE new_order (
  no_w_id INT NOT NULL,
  no_d_id INT NOT NULL,
  no_o_id INT NOT NULL,
  FOREIGN KEY (no_w_id, no_d_id, no_o_id)
    REFERENCES oorder (o_w_id, o_d_id, o_id) ON DELETE CASCADE,
  PRIMARY KEY (no_w_id, no_d_id, no_o_id)
) SPLIT INTO 2 TABLETS;

CREATE TABLE order_line (
  ol_w_id        INT            NOT NULL,
  ol_d_id        INT            NOT NULL,
  ol_o_id        INT            NOT NULL,
  ol_number      INT            NOT NULL,
  ol_i_id        INT            NOT NULL,
  ol_delivery_d  TIMESTAMPTZ(0)          DEFAULT NULL,
  ol_amount      NUMERIC(6,2)   NOT NULL,
  ol_supply_w_id INT            NOT NULL,
  ol_quantity    INT            NOT NULL,
  ol_dist_info   CHAR(24)       NOT NULL,
  FOREIGN KEY (ol_w_id, ol_d_id, ol_o_id)
    REFERENCES oorder (o_w_id, o_d_id, o_id) ON DELETE CASCADE,
  FOREIGN KEY (ol_supply_w_id, ol_i_id)
    REFERENCES stock (s_w_id, s_i_id) ON DELETE CASCADE,
  PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
) SPLIT INTO 2 TABLETS;

CREATE INDEX idx_customer_name
  ON customer (c_w_id, c_d_id, c_last, c_first);
