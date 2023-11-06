CREATE TABLE IF NOT EXISTS ft_cashflow_monthly_by_type_ts (
    agg_date DATE NOT NULL,
    payment_type VARCHAR(25) NOT NULL,
    incoming NUMERIC(10,2) NOT NULL,
    outgoing NUMERIC(10,2) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (agg_date, payment_type)
);