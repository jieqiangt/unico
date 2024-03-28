CREATE TABLE IF NOT EXISTS dim_pdts (
    pdt_code VARCHAR(25) NOT NULL,
    pdt_name VARCHAR(255) NOT NULL,
    foreign_pdt_name VARCHAR(500),
    processed_pdt_ind TINYINT(1),
    uom VARCHAR(50),
    is_active VARCHAR(1),
    new_pdt_ind VARCHAR(3),
    base_price NUMERIC(10, 2),
    warehouse_calculated_avg_price NUMERIC(10, 2),
    pdt_main_category VARCHAR(25),
    last_purchase_date DATE,
    last_purchase_price NUMERIC(10, 2),
    ecommerce_pdt_ind VARCHAR(1),
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pdt_code)
);