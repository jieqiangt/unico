SELECT
    OITW.ItemCode AS 'pdt_code',
    OITM.ItemName AS 'pdt_name',
    OITM.FrgnName AS 'foreign_name',
    OITB.ItmsGrpNam AS 'pdt_main_cat',
    OITM.InvntryUom AS 'uom',
        CASE
            WHEN OITM.QryGroup2 = 'Y' OR OITB.ItmsGrpNam IN ('FISH','SEAFOOD') THEN ROUND(MAX(OITW.AvgPrice) * 1.30 * 2, 0) /2
            ELSE ROUND(MAX(OITW.AvgPrice) * 1.25 * 2, 0) /2
        END AS 'processed_pdt_ind',

    ROUND(MAX(OITW.AvgPrice) * 1.30 * 2, 0) /2 AS 'min_selling_price',
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
    OITB.ItmsGrpNam,
    OITM.QryGroup2
HAVING
    SUM(OITW.OnHand) - SUM(OITW.IsCommited) <> 0;