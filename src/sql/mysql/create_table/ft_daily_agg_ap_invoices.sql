CREATE TABLE IF NOT EXISTS ft_daily_agg_ap_invoices (
    start_of_month DATE NOT NULL,
    doc_date DATE NOT NULL,
    vendor_code VARCHAR(25) NOT NULL,
    sales_employee_code VARCHAR(25) NOT NULL,
    amount NUMERIC(10, 2),
    paid_amount NUMERIC(10, 2),
    outstanding_amount NUMERIC(10, 2),
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (doc_date, vendor_code)
);