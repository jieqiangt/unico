CREATE TABLE IF NOT EXISTS trs_processing_documents (
    doc_num BIGINT PRIMARY KEY AUTO_INCREMENT,
    doc_date DATE NOT NULL,
    created_by TINYINT NOT NULL,
    last_updated_by TINYINT NOT NULL,
    doc_status ENUM('Active', 'Confirmed', 'Deleted') NOT NULL DEFAULT 'Active',
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (last_updated_by) REFERENCES trs_users(id),
    FOREIGN KEY (created_by) REFERENCES trs_users(id)
);