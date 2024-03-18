CREATE TABLE IF NOT EXISTS ft_current_inv_value (
    agg_date DATE NOT NULL,
    as_of_date DATE NOT NULL,
    pdt_code VARCHAR(25) NOT NULL,
    pdt_name VARCHAR(255) NOT NULL,
    uom VARCHAR(25),
    current_inv NUMERIC(10, 2) NOT NULL,
    avg_price NUMERIC(10, 2) NOT NULL,
    total_value NUMERIC(10, 2) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (as_of_date, pdt_code)
);