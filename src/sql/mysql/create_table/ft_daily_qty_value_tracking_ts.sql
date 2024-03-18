CREATE TABLE IF NOT EXISTS ft_daily_qty_value_tracking_ts (
    as_of_date DATE NOT NULL,
    pdt_code VARCHAR(25) NOT NULL,
    value_type VARCHAR(25) NOT NULL,
    value NUMERIC(10, 2) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (as_of_date, pdt_code, value_type)
);