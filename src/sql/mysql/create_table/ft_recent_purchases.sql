CREATE TABLE IF NOT EXISTS ft_recent_purchases (
    doc_date DATE NOT NULL,
    agg_date DATE NOT NULL,
    doc_num VARCHAR(50) NOT NULL,
    line_num SMALLINT NOT NULL,
    supplier_code VARCHAR(25) NOT NULL,
    supplier_name VARCHAR(100) NOT NULL,
    pdt_code VARCHAR(25) NOT NULL,
    pdt_name VARCHAR(100) NOT NULL,
    uom VARCHAR(25),
    qty NUMERIC(10, 2) NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    PRIMARY KEY (doc_date, doc_num, line_num)
);