CREATE TABLE IF NOT EXISTS ft_monthly_pdt_total_value (
    end_of_month DATE NOT NULL,
    pdt_code VARCHAR(25) NOT NULL,
    pdt_name VARCHAR(255) NOT NULL,
    warehouse_code VARCHAR(25) NOT NULL,
    uom VARCHAR(25) NOT NULL,
    last_purchase_date DATE NOT NULL,
    total_value NUMERIC(10, 2) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (end_of_month, pdt_code, warehouse_code)
);