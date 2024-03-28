SELECT
    OITW.ItemCode AS 'pdt_code',
    OITM.FrgnName AS 'foreign_name',
    OITM.ItemName AS 'pdt_name',
    OITB.ItmsGrpNam AS 'pdt_main_cat',
    OITM.InvntryUom AS 'uom',
    SUM(OITW.OnHand) - SUM(OITW.IsCommited) AS 'available_inv',
    OITM.CreateDate AS 'pdt_creation_date',
    CASE
        WHEN DATEDIFF(month, OITM.CreateDate, GETDATE()) < 4 THEN 'new'
        ELSE 'old'
    END AS 'new_pdt_indicator',
    MAX(OITW.AvgPrice) AS 'avg_price',
    MAX(OITM.LastPurDat) AS 'last_purchase_date',
    MAX(OITM.LastPurPrc) AS 'last_purchase_price',
    MAX(OITW.AvgPrice) * 1.25 AS 'breakeven_price'
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
    OITM.CreateDate,
    OITB.ItmsGrpNam
HAVING
    SUM(OITW.OnHand) - SUM(OITW.IsCommited) <> 0;