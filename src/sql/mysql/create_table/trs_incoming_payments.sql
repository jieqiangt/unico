CREATE TABLE IF NOT EXISTS trs_incoming_payments (
    start_of_month DATE NOT NULL,
    doc_num VARCHAR(25) NOT NULL,
    doc_date DATE NOT NULL,
    customer_code VARCHAR(25) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (doc_date, doc_num)
);