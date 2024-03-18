
CREATE TABLE IF NOT EXISTS ft_processed_pdt_daily_output_ts (
    agg_date DATE NOT NULL,
    doc_date DATE NOT NULL,
    pdt_code VARCHAR(25) NOT NULL,
    qty NUMERIC(10,2),
    value NUMERIC(10,2),
    PRIMARY KEY (agg_date, doc_date, pdt_code)
);

