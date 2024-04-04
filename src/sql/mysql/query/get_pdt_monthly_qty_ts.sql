SELECT
    STR_TO_DATE(
        CONCAT(YEAR(as_of_date), '-', MONTH(as_of_date), '-', '1'),
        '%Y-%m-%d'
    ) AS 'agg_date',
    pdt_code,
    value_type AS 'qty_type',
    SUM(value) AS 'qty'
FROM
    ft_daily_qty_value_tracking_ts
WHERE
    value_type IN ('inv_qty', 'sales_qty', 'purchase_qty')
GROUP BY
    STR_TO_DATE(
        CONCAT(YEAR(as_of_date), '-', MONTH(as_of_date), '-', '1'),
        '%Y-%m-%d'
    ),
    pdt_code,
    value_type;