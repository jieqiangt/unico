SELECT
    {{as_of_date}} AS 'as_of_date',
    OITW.ItemCode AS 'pdt_code',
    OITW.WhsCode AS 'warehouse_code',
    (OITW.OnHand - OITW.IsCommited) as 'inv_qty',
    OITW.avgprice AS 'avg_price',
    OITW.avgprice * OITW.OnHand as 'inv_value'
FROM
    OITW;