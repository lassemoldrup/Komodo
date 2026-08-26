CREATE TABLE blackbox_owner.warehouse (
  w_id NUMBER(10,0) NOT NULL,
  w_ytd NUMBER(12,2) NOT NULL,
  w_tax NUMBER(4,4) NOT NULL,
  w_name VARCHAR2(10 CHAR) NOT NULL,
  w_street_1 VARCHAR2(20 CHAR) NOT NULL,
  w_street_2 VARCHAR2(20 CHAR) NOT NULL,
  w_city VARCHAR2(20 CHAR) NOT NULL,
  w_state CHAR(2 CHAR) NOT NULL,
  w_zip CHAR(9 CHAR) NOT NULL,
  CONSTRAINT warehouse_pk PRIMARY KEY (w_id)
);

CREATE TABLE blackbox_owner.item (
  i_id NUMBER(10,0) NOT NULL,
  i_name VARCHAR2(24 CHAR) NOT NULL,
  i_price NUMBER(5,2) NOT NULL,
  i_data VARCHAR2(50 CHAR) NOT NULL,
  i_im_id NUMBER(10,0) NOT NULL,
  CONSTRAINT item_pk PRIMARY KEY (i_id)
);

CREATE TABLE blackbox_owner.stock (
  s_w_id NUMBER(10,0) NOT NULL,
  s_i_id NUMBER(10,0) NOT NULL,
  s_quantity NUMBER(10,0) NOT NULL,
  s_ytd NUMBER(8,2) NOT NULL,
  s_order_cnt NUMBER(10,0) NOT NULL,
  s_remote_cnt NUMBER(10,0) NOT NULL,
  s_data VARCHAR2(50 CHAR) NOT NULL,
  s_dist_01 CHAR(24 CHAR) NOT NULL,
  s_dist_02 CHAR(24 CHAR) NOT NULL,
  s_dist_03 CHAR(24 CHAR) NOT NULL,
  s_dist_04 CHAR(24 CHAR) NOT NULL,
  s_dist_05 CHAR(24 CHAR) NOT NULL,
  s_dist_06 CHAR(24 CHAR) NOT NULL,
  s_dist_07 CHAR(24 CHAR) NOT NULL,
  s_dist_08 CHAR(24 CHAR) NOT NULL,
  s_dist_09 CHAR(24 CHAR) NOT NULL,
  s_dist_10 CHAR(24 CHAR) NOT NULL,
  CONSTRAINT stock_pk PRIMARY KEY (s_w_id,s_i_id)
);

CREATE TABLE blackbox_owner.district (
  d_w_id NUMBER(10,0) NOT NULL,
  d_id NUMBER(10,0) NOT NULL,
  d_ytd NUMBER(12,2) NOT NULL,
  d_tax NUMBER(4,4) NOT NULL,
  d_next_o_id NUMBER(10,0) NOT NULL,
  d_name VARCHAR2(10 CHAR) NOT NULL,
  d_street_1 VARCHAR2(20 CHAR) NOT NULL,
  d_street_2 VARCHAR2(20 CHAR) NOT NULL,
  d_city VARCHAR2(20 CHAR) NOT NULL,
  d_state CHAR(2 CHAR) NOT NULL,
  d_zip CHAR(9 CHAR) NOT NULL,
  CONSTRAINT district_pk PRIMARY KEY (d_w_id,d_id)
);

CREATE TABLE blackbox_owner.customer (
  c_w_id NUMBER(10,0) NOT NULL,
  c_d_id NUMBER(10,0) NOT NULL,
  c_id NUMBER(10,0) NOT NULL,
  c_discount NUMBER(4,4) NOT NULL,
  c_credit CHAR(2 CHAR) NOT NULL,
  c_last VARCHAR2(16 CHAR) NOT NULL,
  c_first VARCHAR2(16 CHAR) NOT NULL,
  c_credit_lim NUMBER(12,2) NOT NULL,
  c_balance NUMBER(12,2) NOT NULL,
  c_ytd_payment BINARY_FLOAT NOT NULL,
  c_payment_cnt NUMBER(10,0) NOT NULL,
  c_delivery_cnt NUMBER(10,0) NOT NULL,
  c_street_1 VARCHAR2(20 CHAR) NOT NULL,
  c_street_2 VARCHAR2(20 CHAR) NOT NULL,
  c_city VARCHAR2(20 CHAR) NOT NULL,
  c_state CHAR(2 CHAR) NOT NULL,
  c_zip CHAR(9 CHAR) NOT NULL,
  c_phone CHAR(16 CHAR) NOT NULL,
  c_since TIMESTAMP(0) NOT NULL,
  c_middle CHAR(2 CHAR) NOT NULL,
  c_data VARCHAR2(500 CHAR) NOT NULL,
  CONSTRAINT customer_pk PRIMARY KEY (c_w_id,c_d_id,c_id)
);
CREATE INDEX blackbox_owner.idx_customer_name
  ON blackbox_owner.customer(c_w_id,c_d_id,c_last,c_first);

CREATE TABLE blackbox_owner.history (
  h_id NUMBER(19,0) GENERATED ALWAYS AS IDENTITY,
  h_c_id NUMBER(10,0) NOT NULL,
  h_c_d_id NUMBER(10,0) NOT NULL,
  h_c_w_id NUMBER(10,0) NOT NULL,
  h_d_id NUMBER(10,0) NOT NULL,
  h_w_id NUMBER(10,0) NOT NULL,
  h_date TIMESTAMP(0) NOT NULL,
  h_amount NUMBER(6,2) NOT NULL,
  h_data VARCHAR2(24 CHAR) NOT NULL,
  CONSTRAINT history_pk PRIMARY KEY (h_id)
);

CREATE TABLE blackbox_owner.oorder (
  o_w_id NUMBER(10,0) NOT NULL,
  o_d_id NUMBER(10,0) NOT NULL,
  o_id NUMBER(10,0) NOT NULL,
  o_c_id NUMBER(10,0) NOT NULL,
  o_carrier_id NUMBER(10,0),
  o_ol_cnt NUMBER(10,0) NOT NULL,
  o_all_local NUMBER(10,0) NOT NULL,
  o_entry_d TIMESTAMP(0) NOT NULL,
  CONSTRAINT oorder_pk PRIMARY KEY (o_w_id,o_d_id,o_id),
  CONSTRAINT oorder_customer_unique UNIQUE (o_w_id,o_d_id,o_c_id,o_id)
);

CREATE TABLE blackbox_owner.new_order (
  no_w_id NUMBER(10,0) NOT NULL,
  no_d_id NUMBER(10,0) NOT NULL,
  no_o_id NUMBER(10,0) NOT NULL,
  CONSTRAINT new_order_pk PRIMARY KEY (no_w_id,no_d_id,no_o_id)
);

CREATE TABLE blackbox_owner.order_line (
  ol_w_id NUMBER(10,0) NOT NULL,
  ol_d_id NUMBER(10,0) NOT NULL,
  ol_o_id NUMBER(10,0) NOT NULL,
  ol_number NUMBER(10,0) NOT NULL,
  ol_i_id NUMBER(10,0) NOT NULL,
  ol_delivery_d TIMESTAMP(0),
  ol_amount NUMBER(6,2) NOT NULL,
  ol_supply_w_id NUMBER(10,0) NOT NULL,
  ol_quantity NUMBER(10,0) NOT NULL,
  ol_dist_info CHAR(24 CHAR) NOT NULL,
  CONSTRAINT order_line_pk PRIMARY KEY (ol_w_id,ol_d_id,ol_o_id,ol_number)
);

BEGIN
  FOR t IN (SELECT column_value AS table_name FROM TABLE(sys.odcivarchar2list(
    'WAREHOUSE','ITEM','STOCK','DISTRICT','CUSTOMER','HISTORY','OORDER','NEW_ORDER','ORDER_LINE'
  ))) LOOP
    EXECUTE IMMEDIATE 'ALTER TABLE blackbox_owner.'||t.table_name||
      ' ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';
    EXECUTE IMMEDIATE 'GRANT SELECT, INSERT, UPDATE, DELETE ON blackbox_owner.'||
      t.table_name||' TO blackbox_application';
    EXECUTE IMMEDIATE 'GRANT SELECT ON blackbox_owner.'||t.table_name||
      ' TO c##blackbox_logminer';
  END LOOP;
END;
/
