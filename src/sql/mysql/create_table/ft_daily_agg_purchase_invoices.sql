CREATE TABLE IF NOT EXISTS ft_daily_agg_purchase_invoices (
    doc_date DATE NOT NULL,
    start_of_month DATE NOT NULL,
    supplier_code VARCHAR(25) NOT NULL,
    pdt_code VARCHAR(25) NOT NULL,
    qty NUMERIC(10, 2) NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    PRIMARY KEY (doc_date, supplier_code, pdt_code)
);