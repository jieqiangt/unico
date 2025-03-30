CREATE TABLE IF NOT EXISTS trs_processing_line_outputs (
    line_output_id BIGINT AUTO_INCREMENT UNIQUE,
    ref_doc_num BIGINT NOT NULL,
    ref_line_id BIGINT NOT NULL,
    pdt_code VARCHAR(255),
    pdt_name VARCHAR(255),
    foreign_name VARCHAR(255),
    uom VARCHAR(50),
    weight NUMERIC(10, 2),
    quantity SMALLINT,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (line_output_id),
    FOREIGN KEY (ref_doc_num) REFERENCES trs_processing_documents(doc_num),
    FOREIGN KEY (ref_line_id) REFERENCES trs_processing_lines(line_id)
);