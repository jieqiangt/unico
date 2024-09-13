CREATE TABLE IF NOT EXISTS ft_monthly_agg_sales_invoices (
    start_of_month DATE NOT NULL,
    customer_code VARCHAR(25) NOT NULL,
    pdt_code VARCHAR(25) NOT NULL,
    sales_employee_code INT NOT NULL,
    qty NUMERIC(10, 2) NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (start_of_month, customer_code, pdt_code, sales_employee_code)
);