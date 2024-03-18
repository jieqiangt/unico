SELECT
    pdt_code,
    SUM(on_hand) AS 'qty'
FROM
    ft_current_inventory
WHERE
    as_of_date = (
        SELECT
            MAX(as_of_date)
        FROM
            ft_current_inventory
    )
GROUP BY
    pdt_code