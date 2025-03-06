CREATE TABLE IF NOT EXISTS trs_processing_line_units (
    line_unit_id INT AUTO_INCREMENT UNIQUE,
    ref_line_id INT NOT NULL,
    pdt_code VARCHAR(255),
    pdt_name VARCHAR(255),
    foreign_name VARCHAR(255),
    uom VARCHAR(50),
    weight NUMERIC(10, 2),
    quantity TINYINT,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (line_unit_id, ref_line_id),
    FOREIGN KEY (ref_line_id) REFERENCES trs_processing_line_items(line_id)
);