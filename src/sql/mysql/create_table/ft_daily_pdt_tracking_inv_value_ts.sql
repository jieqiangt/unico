CREATE TABLE IF NOT EXISTS ft_daily_pdt_tracking_inv_value_ts (
    as_of_date DATE NOT NULL,
    pdt_code VARCHAR(25) NOT NULL,
    inv_value NUMERIC(10,2) NOT NULL,
    daily_pdt_label VARCHAR(25) NOT NULL,
    pdt_name VARCHAR(255) NOT NULL,
    uom VARCHAR(25) NOT NULL,
    current_inv NUMERIC(10,2) NOT NULL,
    monthly_sales_qty NUMERIC(10,2) NOT NULL,
    monthly_sales_qty_to_current_inv_ratio NUMERIC(10,2) NOT NULL,
    last_7_days_sales_ind VARCHAR(10) NOT NULL,
    slow_sales_ind VARCHAR(10) NOT NULL,
    new_pdt_ind VARCHAR(3) NOT NULL,
    PRIMARY KEY (as_of_date, pdt_code)
);