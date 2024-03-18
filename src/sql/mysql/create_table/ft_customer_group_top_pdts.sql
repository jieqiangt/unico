CREATE TABLE IF NOT EXISTS ft_customer_group_top_pdts (
    sales_employee_code INT NOT NULL,
    sales_employee VARCHAR(100) NOT NULL,
    customer_group_name VARCHAR(150) NOT NULL,
    pdt_code VARCHAR(25) NOT NULL,
    pdt_name VARCHAR(150) NOT NULL,
    processed_pdt_ind TINYINT(1) NOT NULL,
    total_profit NUMERIC(10, 2) NOT NULL,
    total_revenue NUMERIC(10, 2) NOT NULL,
    total_qty NUMERIC(10, 2) NOT NULL,
    avg_pc1_margin NUMERIC(10, 2) NOT NULL,
    median_pc1_margin NUMERIC(10, 2) NOT NULL,
    sample_start_date VARCHAR(25) NOT NULL,
    sample_end_date VARCHAR(25) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (
        sales_employee_code,
        customer_group_name,
        pdt_code
    )
);