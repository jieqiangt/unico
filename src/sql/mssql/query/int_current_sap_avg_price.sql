SELECT
    OINM.ItemCode AS 'pdt_code',
    SUM(OINM.TransValue) / SUM(OINM.InQty - OINM.OutQty) AS 'avg_price'
FROM
    OINM
    INNER JOIN OITM ON OINM.ItemCode = OITM.ItemCode
WHERE
    OINM.DocDate <= {{as_of_date}}
GROUP BY
    OINM.ItemCode
HAVING
    SUM(OINM.InQty - OINM.OutQty) <> 0