CREATE TABLE IF NOT EXISTS ft_daily_purchase_value_ts (
    as_of_date DATE NOT NULL,
    amount_with_tax NUMERIC(10, 2) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (as_of_date)
);