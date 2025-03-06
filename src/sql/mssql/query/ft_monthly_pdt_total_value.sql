SELECT
    {{end_of_month}} AS 'end_of_month',
    OINM.ItemCode AS 'pdt_code',
    OITM.ItemName AS 'pdt_name',
    OWHS.WhsCode AS 'warehouse_code',
    OITM.SalUnitMsr 'uom',
    OITM.LastPurDat AS 'last_purchase_date',
    SUM(OINM.TransValue) 'total_value'
FROM
    OINM
    INNER JOIN OWHS ON OWHS.WhsCode = OINM.Warehouse
    INNER JOIN OITM ON OINM.ItemCode = OITM.ItemCode
WHERE
    OINM.DocDate <= {{end_of_month}}
GROUP BY
    OWHS.WhsCode,
    OINM.ItemCode,
    OITM.LastPurDat,
    OITM.SalUnitMsr,
    OITM.ItemName
Having
    SUM(OINM.InQty - OINM.OutQty) <> 0