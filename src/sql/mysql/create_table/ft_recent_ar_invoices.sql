CREATE TABLE IF NOT EXISTS ft_recent_ar_invoices (
    agg_date DATE NOT NULL,
    doc_type VARCHAR(25) NOT NULL,
    doc_status VARCHAR(1) NOT NULL,
    doc_num VARCHAR(25) NOT NULL,
    doc_date DATE NOT NULL,
    customer_code VARCHAR(25) NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    customer_group_name VARCHAR(255) NOT NULL,
    customer_type VARCHAR(25) NOT NULL,
    sales_employee_code VARCHAR(25) NOT NULL,
    sales_employee_name VARCHAR(255) NOT NULL,
    amount_with_tax NUMERIC(10, 2),
    paid_amount NUMERIC(10, 2),
    outstanding_amount NUMERIC(10, 2),
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (
        doc_date,
        doc_num
    )
);