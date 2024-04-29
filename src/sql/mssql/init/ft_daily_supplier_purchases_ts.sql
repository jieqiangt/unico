SELECT
    DATEFROMPARTS(
        YEAR(PCH1.DocDate),
        MONTH(PCH1.DocDate),
        1
    ) AS 'start_of_month_date',
    PCH1.DocDate AS 'as_of_date',
    OPCH.CardCode AS 'supplier_code',
    PCH1.ItemCode AS 'pdt_code',
    OITM.ItemName AS 'pdt_name',
    AVG(PCH1.Price) AS 'purchase_price',
    SUM(PCH1.Quantity) AS 'purchase_qty',
    SUM(PCH1.LineTotal) AS 'purchase_value'
FROM
    PCH1
    LEFT JOIN OITM ON PCH1.ItemCode = OITM.ItemCode
    LEFT JOIN OPCH ON PCH1.DocEntry = OPCH.DocEntry
    LEFT JOIN OCRD ON OPCH.CardCode = OCRD.CardCode
WHERE
    OPCH.Canceled = 'N'
    AND PCH1.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND PCH1.Price > 0.01
    AND ((CHARINDEX('ZS', PCH1.ItemCode) = 0) OR (PCH1.ItemCode IS NULL))
GROUP BY
    PCH1.DocDate,
    OPCH.CardCode,
    PCH1.ItemCode,
    OITM.ItemName
;