CREATE TABLE IF NOT EXISTS dim_process_types (
    id TINYINT PRIMARY KEY AUTO_INCREMENT,
    process_name VARCHAR(50) NOT NULL,
    process_desc VARCHAR(255) NOT NULL,
    is_active BOOLEAN NOT NULL,
    created_by TINYINT NOT NULL,
    updated_by TINYINT NOT NULL,
    created_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (updated_by) REFERENCES trs_users(id),
    FOREIGN KEY (created_by) REFERENCES trs_users(id)
);