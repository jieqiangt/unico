CREATE TABLE IF NOT EXISTS ft_outstanding_ar_breakdown (
    agg_date DATE NOT NULL,
    owed_period VARCHAR(25) NOT NULL,
    customer_code VARCHAR(25) NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    customer_group_name VARCHAR(255) NOT NULL,
    customer_type VARCHAR(25) NOT NULL,
    sales_employee_code VARCHAR(25) NOT NULL,
    sales_employee_name VARCHAR(255) NOT NULL,
    amount_with_tax NUMERIC(10, 2),
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (
        agg_date,
        customer_code,
        sales_employee_code
    )
);