CREATE TABLE IF NOT EXISTS int_pdt_purchase_price_ts (
    as_of_date DATE NOT NULL,
    pdt_code VARCHAR(25) NOT NULL,
    weighted_price NUMERIC(10, 2) NOT NULL,
    previous_price NUMERIC(10, 2) NOT NULL,
    qty_rolling_cum_sum NUMERIC(10, 2) NOT NULL,
    PRIMARY KEY (as_of_date, pdt_code)
);