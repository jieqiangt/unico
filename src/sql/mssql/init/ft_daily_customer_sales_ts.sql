SELECT
    DATEFROMPARTS(
        YEAR(RDR1.DocDate),
        MONTH(RDR1.DocDate),
        1
    ) AS 'start_of_month_date',
    RDR1.DocDate AS 'as_of_date',
    CASE
        WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
        ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
    END AS 'customer_group_name',
    ORDR.CardCode AS 'customer_code',
    RDR1.ItemCode AS 'pdt_code',
    OITM.ItemName AS 'pdt_name',
    AVG(RDR1.Price) AS 'sales_price',
    SUM(RDR1.Quantity) AS 'sales_qty',
    SUM(RDR1.LineTotal) AS 'revenue',
    SUM((RDR1.Price - purchase_price) * RDR1.Quantity) AS 'profit',
    SUM((RDR1.Price - purchase_price) * RDR1.Quantity) / SUM(RDR1.LineTotal) AS 'pc1'
FROM
    RDR1
    LEFT JOIN OITM ON RDR1.ItemCode = OITM.ItemCode
    LEFT JOIN ORDR ON RDR1.DocEntry = ORDR.DocEntry
    LEFT JOIN OCRD ON ORDR.CardCode = OCRD.CardCode
    LEFT JOIN (
        SELECT
            ItemCode,
            AvgPrice AS 'purchase_price'
        FROM
            OITW
        WHERE
            WhsCode = 'FP'
    ) AS pdt_purchase_price ON RDR1.ItemCode = pdt_purchase_price.ItemCode
WHERE
    ORDR.Canceled = 'N'
    AND RDR1.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND RDR1.Price > 0.01
    AND CHARINDEX('ZS', RDR1.ItemCode) = 0
GROUP BY
    RDR1.DocDate,
    CASE
        WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
        ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
    END,
    ORDR.CardCode,
    RDR1.ItemCode,
    OITM.ItemName;
;