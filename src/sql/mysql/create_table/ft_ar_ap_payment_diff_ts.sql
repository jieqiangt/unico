CREATE TABLE IF NOT EXISTS ft_ar_ap_payment_diff_ts (
    agg_date DATE NOT NULL,
    trade_classification VARCHAR(25) NOT NULL,
    ar_total NUMERIC(10, 2) NOT NULL,
    ar_paid NUMERIC(10, 2) NOT NULL,
    ar_outstanding NUMERIC(10, 2) NOT NULL,
    ap_total NUMERIC(10, 2) NOT NULL,
    ap_paid NUMERIC(10, 2) NOT NULL,
    ap_outstanding NUMERIC(10, 2) NOT NULL,
    total_diff NUMERIC(10, 2) NOT NULL,
    outstanding_diff NUMERIC(10, 2) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (
        agg_date,
        trade_classification
    )
);