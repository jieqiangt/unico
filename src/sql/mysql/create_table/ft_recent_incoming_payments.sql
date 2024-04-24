CREATE TABLE IF NOT EXISTS ft_recent_incoming_payments (
    agg_date DATE NOT NULL,
    doc_num VARCHAR(25) NOT NULL,
    doc_date DATE NOT NULL,
    customer_code VARCHAR(25) NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    customer_group_name VARCHAR(255) NOT NULL,
    sales_employee_name VARCHAR(255) NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (
        doc_date,
        doc_num
    )
);