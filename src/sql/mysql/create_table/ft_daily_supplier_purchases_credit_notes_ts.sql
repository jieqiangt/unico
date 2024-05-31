CREATE TABLE IF NOT EXISTS ft_daily_supplier_purchases_credit_notes_ts (
    start_of_month_date DATE NOT NULL,
    as_of_date DATE NOT NULL,
    supplier_code VARCHAR(25) NOT NULL,
    supplier_name VARCHAR(255) NOT NULL,
    pdt_code VARCHAR(25),
    pdt_name VARCHAR(255),
    pdt_main_category VARCHAR(50),
    purchase_price NUMERIC(10,2),
    purchase_value NUMERIC(10,2),
    purchase_qty NUMERIC(10,2),
    credit_note_value NUMERIC(10,2),
    total_value NUMERIC(10,2),
    PRIMARY KEY (as_of_date, pdt_code, supplier_code)
);