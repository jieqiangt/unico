CREATE TABLE IF NOT EXISTS ft_daily_customer_ar_credit_notes_ts (
    start_of_month_date DATE NOT NULL,
    as_of_date DATE NOT NULL,
    customer_group_name VARCHAR(255) NOT NULL,
    customer_code VARCHAR(25) NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    value_type VARCHAR(255),
    value NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (as_of_date, customer_group_name, customer_code, value_type)
);

