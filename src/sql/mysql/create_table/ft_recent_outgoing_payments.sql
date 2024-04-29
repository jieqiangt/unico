CREATE TABLE IF NOT EXISTS ft_recent_outgoing_payments (
    agg_date DATE NOT NULL,
    doc_num VARCHAR(25) NOT NULL,
    doc_date DATE NOT NULL,
    supplier_code VARCHAR(25) NOT NULL,
    supplier_name VARCHAR(255) NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (
        doc_date,
        doc_num
    )
);