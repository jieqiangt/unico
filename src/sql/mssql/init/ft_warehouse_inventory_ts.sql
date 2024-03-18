SELECT
    {{as_of_date}} AS 'as_of_date',
    OITM.ItemCode AS 'pdt_code',
    OITM.SalUnitMsr AS 'uom',
    COALESCE(OINM.Warehouse, 'FP') AS 'warehouse_code',
    COALESCE(SUM(OINM.InQty - OINM.OutQty), 0) AS 'on_hand',
    0 AS 'is_committed',
    0 AS 'on_order',
    0 AS 'consig',
    0 AS 'counted',
    0 AS 'was_counted'
FROM
    OITM
    LEFT JOIN OINM ON OITM.ItemCode = OINM.ItemCode
WHERE
    OINM.DocDate <= {{as_of_date}}
GROUP BY
    OITM.ItemCode,
    OITM.ItemName,
    OITM.SalUnitMsr,
    OINM.Warehouse
HAVING
    SUM(OINM.InQty - OINM.OutQty) <> 0;