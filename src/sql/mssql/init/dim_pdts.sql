SELECT
    OITM.ItemCode AS 'pdt_code',
	OITM.ItemName AS 'pdt_name',
    OITM.FrgnName AS 'foreign_pdt_name',
	OITM.InvntryUoM as 'uom',
    CASE
        WHEN OITM.QryGroup2 = 'Y' THEN 1
        ELSE 0
    END AS 'processed_pdt_ind',
    OITM.ValidFor AS 'is_active',
    CASE
        WHEN DATEDIFF(week, OITM.CreateDate, GETDATE()) < 3 THEN 'new'
        ELSE 'old'
    END AS 'new_pdt_ind',
    OITM.LstEvlPric AS 'base_price',
    OITB.ItmsGrpNam AS 'pdt_main_category'
FROM
    OITM
LEFT JOIN OITB ON OITM.ItmsGrpCod = OITB.ItmsGrpCod
;