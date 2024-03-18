CREATE TABLE IF NOT EXISTS dim_customers (
    customer_code VARCHAR(25),
    name VARCHAR(100) NOT NULL,
    customer_group_name VARCHAR(100) NOT NULL,
    sales_employee VARCHAR(100) NOT NULL,
    entity_type VARCHAR(1) NOT NULL,
    address VARCHAR(255),
    zipcode VARCHAR(255),
    industry VARCHAR(100),
    trade_ind VARCHAR(25) NOT NULL,
    payment_terms VARCHAR(25) NOT NULL,
    is_active BOOLEAN NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer_code)
);