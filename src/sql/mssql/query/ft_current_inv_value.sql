SELECT
    DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AS 'agg_date',
    {{as_of_date}} AS 'as_of_date',
    OITW.ItemCode AS 'pdt_code',
    OITM.ItemName 'pdt_name',
    OITM.SalUnitMsr 'uom',
    (OITW.OnHand - OITW.IsCommited) as 'current_inv',
    OITW.avgprice AS 'avg_price',
    OITW.avgprice * OITW.OnHand as 'total_value'
FROM
    OITW
    INNER JOIN OITM ON OITW.ItemCode = OITM.ItemCode
WHERE 
    OITW.WhsCode = 'FP';



