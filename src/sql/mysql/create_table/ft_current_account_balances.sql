CREATE TABLE IF NOT EXISTS ft_current_account_balances (
    agg_date DATE NOT NULL,
    as_of_date DATE NOT NULL,
    account_code VARCHAR(25) NOT NULL,
    account_name VARCHAR(255) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (as_of_date, account_code)
);