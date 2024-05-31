CREATE TABLE IF NOT EXISTS ft_daily_pdt_tracking_inv_value_ts (
    as_of_date DATE NOT NULL,
    pdt_code VARCHAR(25) NOT NULL,
    inv_value NUMERIC(10, 2) NOT NULL,
    daily_pdt_label VARCHAR(25) NOT NULL,
    PRIMARY KEY (as_of_date, pdt_code)
);