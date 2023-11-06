CREATE TABLE IF NOT EXISTS ft_sales_agent_performance_ts (
    agg_date DATE NOT NULL,
    sales_employee_code INT NOT NULL,
    sales_employee VARCHAR(100) NOT NULL,
    sales_amount NUMERIC(10,2) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (agg_date, sales_employee)
);