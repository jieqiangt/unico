CREATE TABLE IF NOT EXISTS ft_daily_pdt_processing_movement_ts (
    as_of_date DATE NOT NULL,
    qty_to_processing NUMERIC(10,2) NOT NULL,
    qty_from_processing NUMERIC(10,2) NOT NULL,
    value_to_processing NUMERIC(10,2) NOT NULL,
    value_from_processing NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (as_of_date)
);