CREATE TABLE IF NOT EXISTS trs_processing_line (
    reference_doc_num VARCHAR(255) NOT NULL,
    line_id VARCHAR(255) UNIQUE,
    pdt_code VARCHAR(50) NOT NULL,
    total_weight NUMERIC(10,2) NOT NULL,
    qty NUMERIC(10,2) NOT NULL,
    uom VARCHAR(25) NOT NULL,
    line_item_type VARCHAR(25) NOT NULL,
    reference_line_id VARCHAR(255) NOT NULL,
    created_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255) NOT NULL,
    updated_before BOOLEAN NOT NULL,
    updated_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(255),
    PRIMARY KEY (reference_doc_num, line_id)
);


