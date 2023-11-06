WITH inv AS (
    SELECT
        OITW.ItemCode AS 'pdt_code',
        SUM(OITW.OnHand + OITW.IsCommited) AS 'total_inv'
    FROM
        OITW
    GROUP BY
        OITW.ItemCode
    HAVING
        SUM(OITW.OnHand + OITW.IsCommited) > 0
),
price AS (
    SELECT
        ItemCode AS 'pdt_code',
        LstEvlPric AS 'price'
    FROM
        OITM
)
SELECT
    DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AS 'agg_date',
    {{as_of_date}} AS 'as_of_date',
    inv.pdt_code,
    total_inv * price AS 'total_value'
FROM
    inv
    LEFT JOIN price ON inv.pdt_code = price.pdt_code;