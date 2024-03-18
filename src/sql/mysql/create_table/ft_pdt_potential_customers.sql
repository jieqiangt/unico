CREATE TABLE IF NOT EXISTS ft_pdt_potential_customers (
    pdt_code VARCHAR(25) NOT NULL,
    customer_code VARCHAR(25) NOT NULL,
    total_sales_value NUMERIC(10,2) NOT NULL,
    num_sales_orders NUMERIC(10,2) NOT NULL,
    median_sales_value NUMERIC(10,2) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pdt_code, customer_code)
);