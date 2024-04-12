SELECT
    DATEFROMPARTS(
        YEAR(RDR1.DocDate),
        MONTH(RDR1.DocDate),
        1
    ) AS 'agg_date',
    CASE
        WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
        ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
    END AS 'customer_group_name',
    ORDR.SlpCode AS 'sales_employee_code',
    OSLP.SlpName AS 'sales_employee_name',
    RDR1.ItemCode AS 'pdt_code',
    OITM.ItemName AS 'pdt_name',
    SUM(RDR1.LineTotal) AS 'revenue',
    SUM((RDR1.Price - purchase_price) * RDR1.Quantity) AS 'profit',
    SUM((RDR1.Price - purchase_price) * RDR1.Quantity) / SUM(RDR1.LineTotal) AS 'pc1'
FROM
    RDR1
    LEFT JOIN OITM ON RDR1.ItemCode = OITM.ItemCode
    LEFT JOIN ORDR ON RDR1.DocEntry = ORDR.DocEntry
    LEFT JOIN OCRD ON ORDR.CardCode = OCRD.CardCode
    LEFT JOIN OSLP ON ORDR.SlpCode = OSLP.SlpCode
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
    DATEFROMPARTS(
        YEAR(RDR1.DocDate),
        MONTH(RDR1.DocDate),
        1
    ),
    RDR1.ItemCode,
    OITM.ItemName,
    CASE
        WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
        ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
    END,
    ORDR.SlpCode,
    OSLP.SlpName;