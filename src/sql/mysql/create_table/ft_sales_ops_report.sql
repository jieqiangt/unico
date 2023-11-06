CREATE TABLE IF NOT EXISTS ft_sales_ops_report (
    pdt_code VARCHAR(25) NOT NULL,
    pdt_name VARCHAR(255) NOT NULL,
    current_inv NUMERIC(10, 2),
    purchases_data_date_range VARCHAR(100),
    purchases_min_price NUMERIC(10, 2),
    purchases_max_price NUMERIC(10, 2),
    sales_data_date_range VARCHAR(100),
    latest_sales_date DATE,
    latest_sales_price NUMERIC(10, 2),
    sales_min_price NUMERIC(10, 2),
    sales_max_price NUMERIC(10, 2),
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pdt_code)
);