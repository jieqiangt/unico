CREATE TABLE IF NOT EXISTS trs_sales_invoices (
    doc_date DATE NOT NULL,
    start_of_month DATE NOT NULL,
    doc_num VARCHAR(50) NOT NULL,
    line_num SMALLINT NOT NULL,
    customer_code VARCHAR(25) NOT NULL,
    pdt_code VARCHAR(25) NOT NULL,
    sales_employee_code INT NOT NULL,
    qty NUMERIC(10, 2) NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    PRIMARY KEY (doc_date, doc_num, line_num)
);