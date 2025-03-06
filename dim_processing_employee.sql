CREATE TABLE IF NOT EXISTS dim_processing_employee (
    emp_id VARCHAR(255) UNIQUE,
    emp_num SERIAL,
    emp_name VARCHAR(50) NOT NULL,
    emp_role VARCHAR(50) NOT NULL,
    deleted BOOLEAN NOT NULL,
    is_active BOOLEAN NOT NULL,
    created_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(50) NOT NULL,
    updated_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(50) NOT NULL,
    sap_id VARCHAR(50),
    PRIMARY KEY (emp_id, emp_num)
);
