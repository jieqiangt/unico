CREATE TABLE IF NOT EXISTS ft_suppliers_monthly_pv_ts (
    agg_date DATE NOT NULL,
    overseas_local_ind VARCHAR(50) NOT NULL,
    trade_ind VARCHAR(25) NOT NULL,
    is_active BOOLEAN NOT NULL,
    purchase_value NUMERIC(10,2) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (agg_date, overseas_local_ind, trade_ind, is_active)
);