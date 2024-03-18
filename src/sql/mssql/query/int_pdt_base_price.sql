SELECT
    ItemCode AS 'pdt_code',
    ItemName AS 'pdt_name',
    InvntryUom AS 'uom',
    LstEvlPric AS 'base_price',
    CASE
        WHEN DATEDIFF(week, OITM.CreateDate, GETDATE()) < 3 THEN 'new'
        ELSE 'old'
    END AS 'new_pdt_indicator'
FROM
    OITM;