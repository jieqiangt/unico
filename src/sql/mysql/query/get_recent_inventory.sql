SELECT
    as_of_date,
    pdt_code,
    SUM(on_hand) AS 'qty'
FROM
    ft_warehouse_inventory_ts
WHERE
    as_of_date BETWEEN {{start_date}} AND {{end_date}}
GROUP BY
    as_of_date,
    pdt_code;