CREATE TABLE IF NOT EXISTS dim_suppliers (
    supplier_code VARCHAR(25) NOT NULL,
    name VARCHAR(100) NOT NULL,
    entity_type VARCHAR(1),
    address VARCHAR(255),
    zipcode VARCHAR(255),
    overseas_local_ind VARCHAR(50),
    trade_ind VARCHAR(25),
    payment_terms VARCHAR(25),
    is_active BOOLEAN,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (supplier_code)
);