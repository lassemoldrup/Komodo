\set ON_ERROR_STOP on
SET search_path TO benchbase;

-- Boundary corpus: one marker-terminated transaction touching all nine TPC-C
-- tables with decimal sign/scale/trailing-zero, float32, UTC whole-second
-- timestamp, CHAR padding, nullable, empty-value, and UTF-8 boundaries.
BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO warehouse (
    w_id, w_ytd, w_tax, w_name, w_street_1, w_street_2, w_city, w_state, w_zip
) VALUES (
    1, 300000.00, 0.1234, '仓库  ', '', 'Deux', 'München', 'C', '12345'
);
INSERT INTO item (i_id, i_name, i_price, i_data, i_im_id)
VALUES (1, 'ordinary supply item', 1.00, 'plain data', 1);
INSERT INTO item (i_id, i_name, i_price, i_data, i_im_id)
VALUES (500, 'boundary item name 24ch.', 999.99, 'ωmega data', 2147483647);
INSERT INTO stock (
    s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data,
    s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
    s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
) VALUES (
    1, 1, 91, 0.00, 0, 0, 'stock data',
    'd', 'exactly-twenty-four-chr.', 'dist 03', 'dist 04', 'dist 05',
    'dist 06', 'dist 07', 'dist 08', 'dist 09', 'dist 10'
);
INSERT INTO district (
    d_w_id, d_id, d_ytd, d_tax, d_next_o_id, d_name,
    d_street_1, d_street_2, d_city, d_state, d_zip
) VALUES (
    1, 1, 30000.00, 0.0000, 3001, 'Dist一', 'Rue', '', 'Lyon', 'LY', '987654321'
);
INSERT INTO customer (
    c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first,
    c_credit_lim, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt,
    c_street_1, c_street_2, c_city, c_state, c_zip, c_phone,
    c_since, c_middle, c_data
) VALUES (
    1, 1, 1, 0.9999, 'GC', 'BARBARBAR', 'Émile',
    50000.00, -10.00, 0.1, 1, 0,
    'Street 1', 'Street 2', 'Paris', 'PA', '123456789', '0123456789012345',
    TIMESTAMPTZ '2000-01-01 00:00:00+00', 'OE', 'history Ω payload'
);
INSERT INTO history (
    h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data
) VALUES (
    1, 1, 1, 1, 1, TIMESTAMPTZ '2000-01-01 00:00:00+00', 10.00, 'payment ω data'
);
INSERT INTO oorder (
    o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d
) VALUES
    (1, 1, 2999, 1, 5, 1, 1, TIMESTAMPTZ '2026-08-24 01:02:03+00'),
    (1, 1, 3000, 1, NULL, 2, 1, TIMESTAMPTZ '2026-08-24 01:02:03+00');
INSERT INTO new_order (no_w_id, no_d_id, no_o_id)
VALUES (1, 1, 3000);
INSERT INTO order_line (
    ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d,
    ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info
) VALUES
    (1, 1, 3000, 1, 1, NULL, 0.00, 1, 5, 'district info line one'),
    (1, 1, 3000, 2, 1, TIMESTAMPTZ '2026-01-02 03:04:05+00', 9999.99, 1, 10,
     'exactly-twenty-four-chr.');
INSERT INTO session_txn (session_id, txn_id)
VALUES ('tpcc/capture/1', 1)
ON CONFLICT (session_id) DO UPDATE SET txn_id = EXCLUDED.txn_id;
COMMIT;
\echo ACK tpcc_value_normalization 1
