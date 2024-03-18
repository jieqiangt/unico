CREATE TABLE IF NOT EXISTS ft_warehouse_inventory_ts (
    as_of_date DATE NOT NULL,
    pdt_code VARCHAR(25) NOT NULL,
    uom VARCHAR(10),
    warehouse_code VARCHAR(25),
    on_hand NUMERIC(10,2) NOT NULL,
    is_committed NUMERIC(10,2) NOT NULL,
    on_order NUMERIC(10,2) NOT NULL,
    consig NUMERIC(10,2) NOT NULL,
    was_counted BOOLEAN NOT NULL,
    counted NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (as_of_date, pdt_code, warehouse_code)
);