SELECT
    {{as_of_date}} AS 'as_of_date',
    OITW.ItemCode AS 'pdt_code',
    OITM.ItemName AS 'pdt_name',
    OITM.SalUnitMsr As 'uom',
    AVG(OITW.AvgPrice) AS 'warehouse_avg_price',
    SUM(OITW.OnHand) AS 'on_hand_qty',
    SUM(OITW.OnHand) * AVG(OITW.AvgPrice) AS 'on_hand_value',
    SUM(OITW.IsCommited) AS 'is_commited_qty',
    SUM(OITW.IsCommited) * AVG(OITW.AvgPrice) AS 'is_commited_value',
    SUM(OITW.OnOrder) AS 'on_order_qty',
    SUM(OITW.OnOrder) * AVG(OITW.AvgPrice) AS 'on_order_value',
    SUM(OITW.Consig) AS 'consig_qty',
    SUM(OITW.Consig) * AVG(OITW.AvgPrice) AS 'consig_value'
FROM    
    OITW
    LEFT JOIN OITM ON OITW.ItemCode = OITM.ItemCode
GROUP BY
    OITW.ItemCode,
    OITM.ItemName,
    OITM.SalUnitMsr;