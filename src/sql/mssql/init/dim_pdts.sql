SELECT
    ItemCode AS 'pdt_code',
	ItemName AS 'pdt_name',
    FrgnName AS 'foreign_pdt_name',
	InvntryUoM as 'uom',
    CASE
        WHEN QryGroup2 = 'Y' THEN 1
        ELSE 0
    END AS 'processed_pdt_ind',
    ValidFor AS 'is_active',
    CASE
        WHEN DATEDIFF(week, OITM.CreateDate, GETDATE()) < 3 THEN 'new'
        ELSE 'old'
    END AS 'new_pdt_indicator'
FROM
    OITM
;