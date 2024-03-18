SELECT
    {{as_of_date}} AS 'as_of_date',
    pdt_code,
    uom,
    SUM(on_hand) AS 'on_hand',
    SUM(is_committed) AS 'is_committed',
    SUM(on_order) AS 'on_order'
FROM
    ft_warehouse_inventory_ts
WHERE
    as_of_date IN (
        SELECT
            MAX(as_of_date)
        FROM
            ft_warehouse_inventory_ts
    )
GROUP BY
    pdt_code,
    uom