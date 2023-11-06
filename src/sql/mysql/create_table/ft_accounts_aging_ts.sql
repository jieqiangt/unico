CREATE TABLE IF NOT EXISTS ft_accounts_aging_ts (
    agg_date DATE NOT NULL,
    account_type VARCHAR(25) NOT NULL,
    payment_status VARCHAR(1) NOT NULL,
    trade_classification VARCHAR(25) NOT NULL,
    amount_type VARCHAR(25) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (
        agg_date,
        account_type,
        payment_status,
        trade_classification,
        amount_type
    )
);