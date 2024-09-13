SELECT
    OITW.ItemCode AS 'pdt_code',
    OITM.ItemName AS 'pdt_name',
    OITM.FrgnName AS 'foreign_name',
    OITB.ItmsGrpNam AS 'pdt_main_cat',
    OITM.InvntryUom AS 'uom',
    MAX(OITW.AvgPrice) * 1.25 AS 'breakeven_price',
    ROUND(MAX(OITW.AvgPrice) * 1.35 * 2, 0) / 2 AS 'rsp'
FROM
    OITW
    LEFT JOIN OITM ON OITW.ItemCode = OITM.ItemCode
    LEFT JOIN OITB ON OITM.ItmsGrpCod = OITB.ItmsGrpCod
WHERE
    OITW.ItemCode NOT LIKE 'ZS%'
GROUP BY
    OITW.ItemCode,
    OITM.ItemName,
    OITM.FrgnName,
    OITM.InvntryUom,
    OITB.ItmsGrpNam
HAVING
    SUM(OITW.OnHand) - SUM(OITW.IsCommited) <> 0;