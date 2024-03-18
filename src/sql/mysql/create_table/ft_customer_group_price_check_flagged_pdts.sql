

CREATE TABLE IF NOT EXISTS ft_customer_group_price_check_flagged_pdts (
    as_of_date DATE NOT NULL,
    customer_group_name VARCHAR(255) NOT NULL,
    pdt_code VARCHAR(25) NOT NULL,
    pdt_name VARCHAR(255) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (as_of_date, customer_group_name, pdt_code, pdt_name)
);