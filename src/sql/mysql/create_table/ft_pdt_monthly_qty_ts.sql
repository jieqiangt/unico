CREATE TABLE IF NOT EXISTS ft_pdt_monthly_qty_ts (
    agg_date DATE NOT NULL,
    pdt_code VARCHAR(25) NOT NULL,
    qty_type VARCHAR(25) NOT NULL,
    qty NUMERIC(10, 2) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (agg_date, pdt_code, qty_type)
);