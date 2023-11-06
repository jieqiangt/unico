SELECT
    pdt_code,
    SUM(on_hand) AS 'on_hand',
    SUM(is_committed) AS 'is_committed',
    SUM(on_order) AS 'on_order'
FROM
    ft_current_inventory
WHERE
    as_of_date = {{as_of_date}}
GROUP BY
    pdt_code