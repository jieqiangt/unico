CREATE TABLE IF NOT EXISTS dim_recent_processed_pdts (
    pdt_code VARCHAR(25) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pdt_code)
);