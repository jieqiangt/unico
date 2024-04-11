SELECT
    as_of_date,
    'Inventory' AS 'value_category',
    'INVENTORY' AS 'value_sub_category',
    SUM(inv_value) AS 'value'
FROM
    ft_warehouse_inventory_ts
WHERE
    as_of_date BETWEEN {{start_date}} AND {{end_date}}
GROUP BY
    as_of_date