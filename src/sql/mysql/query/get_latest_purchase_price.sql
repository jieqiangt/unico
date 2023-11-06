SELECT
    as_of_date,
    pdt_code,
    weighted_price,
    previous_price
FROM
    int_pdt_purchase_price_ts
WHERE
    as_of_date = {{last_as_of_date}};