CREATE TABLE IF NOT EXISTS trs_processing_main (
    doc_id VARCHAR(255) UNIQUE,
    doc_num SERIAL,
    doc_date DATE NOT NULL,
    scheduled_date DATE NOT NULL,
    cancelled BOOLEAN NOT NULL,
    is_closed VARCHAR(1) NOT NULL,
    processing_status VARCHAR(25) NOT NULL,
    created_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255) NOT NULL,
    updated_before BOOLEAN NOT NULL,
    updated_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(255),
    weight_loss NUMERIC(10,2) NOT NULL,
    process_type VARCHAR(25) NOT NULL,
    processed_by VARCHAR(25) NOT NULL,
    processing_start_time DATETIME NOT NULL,
    processing_end_time DATETIME,
    PRIMARY KEY (doc_id, doc_num)
);

