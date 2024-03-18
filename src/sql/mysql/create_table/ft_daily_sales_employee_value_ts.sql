CREATE TABLE IF NOT EXISTS ft_daily_sales_employee_value_ts (
    as_of_date DATE NOT NULL,
    agg_date DATE NOT NULL,
    sales_employee_code VARCHAR(25) NOT NULL,
    sales_employee_name VARCHAR(255) NOT NULL,
    value NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (as_of_date, sales_employee_code, sales_employee_name)
);