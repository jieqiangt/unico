CREATE TABLE IF NOT EXISTS ft_monthly_agg_ar_invoices (
    start_of_month DATE NOT NULL,
    customer_code VARCHAR(25) NOT NULL,
    sales_employee_code VARCHAR(25) NOT NULL,
    amount NUMERIC(10, 2),
    paid_amount NUMERIC(10, 2),
    outstanding_amount NUMERIC(10, 2),
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (start_of_month, customer_code, sales_employee_code)
);