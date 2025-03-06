CREATE TABLE IF NOT EXISTS ft_sales_orders_ar_investigation (
    ar_invoice_date DATE NOT NULL,
    ar_invoice_doc_num VARCHAR(50) NOT NULL,
    ar_gross_profit NUMERIC(10, 2) NOT NULL,
    ar_status VARCHAR(10) NOT NULL,
    sales_order_date DATE NOT NULL,
    sales_order_doc_num VARCHAR(50) NOT NULL,
    sales_order_gross_profit NUMERIC(10, 2) NOT NULL,
    sales_order_status VARCHAR(10) NOT NULL,
    originating_doc_type VARCHAR(10) NOT NULL,
    payment_terms VARCHAR(50) NOT NULL,
    sales_employee VARCHAR(50) NOT NULL,
    customer_code VARCHAR(50) NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ar_invoice_date, ar_invoice_doc_num, sales_order_date, sales_order_doc_num)
);