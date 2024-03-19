CREATE TABLE IF NOT EXISTS ft_current_processing_movement (
    movement_doc_num VARCHAR(25) NOT NULL,
    doc_date DATE NOT NULL,
    pdt_code VARCHAR(25) NOT NULL,
    pdt_name VARCHAR(255) NOT NULL,
    uom VARCHAR(25) NOT NULL,
    qty NUMERIC(10, 2) NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    movement_type VARCHAR(50) NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);