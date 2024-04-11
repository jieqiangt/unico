CREATE TABLE IF NOT EXISTS ft_daily_agg_values_ts (
    as_of_date DATE NOT NULL,
    value_category VARCHAR(25) NOT NULL,
    value_sub_category VARCHAR(25) NOT NULL,
    value NUMERIC(10, 2) NOT NULL,
    daily_value_label VARCHAR(25) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (as_of_date, value_category, value_sub_category)
);