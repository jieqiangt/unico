CREATE TABLE IF NOT EXISTS trs_processing_lines (
    line_id INT AUTO_INCREMENT UNIQUE,
    ref_doc_num INT NOT NULL,
    processed_by TINYINT,
    process_start DATETIME,
    process_end DATETIME,
    process_type TINYINT,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (line_id, ref_doc_num),
    FOREIGN KEY (ref_doc_num) REFERENCES trs_processing_documents(doc_num),
    FOREIGN KEY (processed_by) REFERENCES trs_users(id),
    FOREIGN KEY (process_type) REFERENCES dim_process_types(id)
);