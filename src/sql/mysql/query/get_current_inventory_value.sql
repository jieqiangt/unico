SELECT
    pdt_code,
    SUM(inv_qty) as 'current_inv_qty',
    SUM(inv_value) as 'current_inv_value'
FROM
    ft_warehouse_inventory_ts_new
WHERE
    as_of_date IN (
        SELECT
            MAX(as_of_date)
        FROM
            ft_warehouse_inventory_ts_new
    )
GROUP BY
    pdt_code;