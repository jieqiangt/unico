SELECT
    pdt_code,
    weighted_price
FROM
    int_pdt_purchase_price_ts
WHERE
    as_of_date IN (
        SELECT
            MAX(as_of_date)
        FROM
            int_pdt_purchase_price_ts
    )