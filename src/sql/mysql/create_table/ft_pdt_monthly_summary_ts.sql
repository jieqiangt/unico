CREATE TABLE IF NOT EXISTS ft_pdt_monthly_summary_ts (
    agg_date DATE NOT NULL,
    pdt_code VARCHAR(25) NOT NULL,
    transaction_type VARCHAR(10) NOT NULL,
    price NUMERIC(10,2),
    qty NUMERIC(10,2),
    PRIMARY KEY (agg_date, pdt_code, transaction_type)
);