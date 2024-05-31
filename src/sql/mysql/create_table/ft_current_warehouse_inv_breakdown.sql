CREATE TABLE IF NOT EXISTS ft_current_warehouse_inv_breakdown (
    as_of_date DATE NOT NULL,
    pdt_code VARCHAR(25) NOT NULL,
    pdt_name VARCHAR(255) NOT NULL,
    uom VARCHAR(25),
    on_hand_qty NUMERIC(10, 2) NOT NULL,
    on_hand_value NUMERIC(10, 2) NOT NULL,
    is_commited_qty NUMERIC(10, 2) NOT NULL,
    is_commited_value NUMERIC(10, 2) NOT NULL,
    on_order_qty NUMERIC(10, 2) NOT NULL,
    on_order_value NUMERIC(10, 2) NOT NULL,
    consig_qty NUMERIC(10, 2) NOT NULL,
    consig_value NUMERIC(10, 2) NOT NULL,
    warehouse_avg_price NUMERIC(10, 2) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (as_of_date, pdt_code)
);