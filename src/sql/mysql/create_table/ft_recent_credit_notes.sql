CREATE TABLE IF NOT EXISTS ft_recent_credit_notes (
    doc_date DATE NOT NULL,
    agg_date DATE NOT NULL,
    doc_num VARCHAR(50) NOT NULL,
    line_num SMALLINT NOT NULL,
    customer_code VARCHAR(25) NOT NULL,
    customer_name VARCHAR(150) NOT NULL,
    customer_group_name VARCHAR(150) NOT NULL,
    pdt_code VARCHAR(25) NOT NULL,
    pdt_name VARCHAR(150) NOT NULL,
    uom VARCHAR(25),
    sales_employee_code INT NOT NULL,
    sales_employee VARCHAR(100) NOT NULL,
    qty NUMERIC(10, 2) NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    PRIMARY KEY (doc_date, doc_num, line_num)
);