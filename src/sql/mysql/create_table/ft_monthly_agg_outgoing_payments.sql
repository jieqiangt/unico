CREATE TABLE IF NOT EXISTS ft_monthly_agg_outgoing_payments (
    start_of_month DATE NOT NULL,
    supplier_code VARCHAR(25) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (start_of_month, supplier_code)
);