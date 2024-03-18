CREATE TABLE IF NOT EXISTS ft_warehouse_inventory_ts_new (
    as_of_date DATE NOT NULL,
    pdt_code VARCHAR(25) NOT NULL,
    warehouse_code VARCHAR(25),
    inv_qty NUMERIC(10,2) NOT NULL,
    inv_value NUMERIC(10,2) NOT NULL,
    avg_price NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (as_of_date, pdt_code, warehouse_code)
);