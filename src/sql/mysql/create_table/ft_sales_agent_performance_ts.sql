CREATE TABLE IF NOT EXISTS ft_sales_agent_performance_ts (
    agg_date DATE NOT NULL,
    pdt_code VARCHAR(25) NOT NULL,
    pdt_name VARCHAR(255) NOT NULL,
    customer_group_name VARCHAR(255) NOT NULL,
    payment_terms VARCHAR(255) NOT NULL,
    sales_employee_code VARCHAR(25) NOT NULL,
    sales_employee_name VARCHAR(255) NOT NULL,
    revenue NUMERIC(10,2) NOT NULL,
    profit NUMERIC(10,2) NOT NULL,
    pc1 NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (agg_date, pdt_code, customer_group_name, sales_employee_code)
);