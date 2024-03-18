CREATE TABLE IF NOT EXISTS ft_customer_churn (
    customer_code VARCHAR(25) NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    activity VARCHAR(50) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer_code)
);