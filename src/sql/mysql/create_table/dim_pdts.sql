CREATE TABLE IF NOT EXISTS dim_pdts (
    pdt_code VARCHAR(25) NOT NULL,
    pdt_name VARCHAR(255) NOT NULL,
    foreign_pdt_name VARCHAR(500),
    processed_pdt_ind TINYINT(1),
    uom VARCHAR(50),
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pdt_code)
);