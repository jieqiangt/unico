SELECT
    {{as_of_date}} AS 'as_of_date',
    OINM.ItemCode AS 'pdt_code',
    OINM.Warehouse AS 'warehouse_code',
    SUM(OINM.InQty - OINM.OutQty) AS 'inv_qty',
    IIF(
        SUM(OINM.InQty - OINM.OutQty) = 0,
        0,
        (
            SUM(OINM.TransValue) / SUM(OINM.InQty - OINM.OutQty)
        )
    ) AS 'avg_price',
    SUM(OINM.TransValue) AS 'inv_value'
FROM
    OINM
WHERE
    OINM.DocDate <= {{as_of_date}}
GROUP BY
    OINM.Warehouse,
    OINM.ItemCode;