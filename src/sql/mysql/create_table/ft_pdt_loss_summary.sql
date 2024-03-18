CREATE TABLE IF NOT EXISTS ft_pdt_loss_summary (
    pdt_code VARCHAR(25) NOT NULL,
    total_profit_loss NUMERIC(10,2),
    num_losses NUMERIC(10,2),
    total_losses NUMERIC(10,2),
    sample_start_date DATETIME NOT NULL,
    sample_end_date DATETIME NOT NULL,
    created_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pdt_code)
);