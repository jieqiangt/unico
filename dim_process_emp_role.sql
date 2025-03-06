CREATE TABLE IF NOT EXISTS dim_process_emp_role (
    role_id VARCHAR(255) UNIQUE,
    role_num SERIAL,
    role_name VARCHAR(50) NOT NULL,
    role_description VARCHAR(255) NOT NULL,
    deleted BOOLEAN NOT NULL,
    is_active BOOLEAN NOT NULL,
    created_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(50) NOT NULL,
    updated_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(50) NOT NULL,
    PRIMARY KEY (role_id, role_num)
);
