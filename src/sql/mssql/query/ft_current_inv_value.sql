SELECT
    DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AS 'agg_date',
    {{as_of_date}} AS 'as_of_date',
    OINM.ItemCode AS 'pdt_code',
    OITM.ItemName 'pdt_name',
    OITM.SalUnitMsr 'uom',
    SUM(OINM.InQty - OINM.OutQty) 'current_inv',
    SUM(OINM.TransValue) / SUM(OINM.InQty - OINM.OutQty) AS 'avg_price',
    SUM(OINM.TransValue) AS 'total_value'
FROM
    OINM
    INNER JOIN OITM ON OINM.ItemCode = OITM.ItemCode
WHERE
    OINM.DocDate <= {{as_of_date}}
GROUP BY
    OINM.ItemCode,
	OITM.ItemName,
    OITM.SalUnitMsr
HAVING
    SUM(OINM.InQty - OINM.OutQty) <> 0;