SELECT
    ORDR.DocNum AS 'doc_num',
    TRIM(CONVERT(char, RDR1.DocDate, 112)) AS 'doc_date',
    RDR1.LineNum AS 'line_num',
    ORDR.CardCode AS 'customer_code',
    OCRD.CardName AS 'customer_name',
    CASE
        WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
        ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
    END AS 'customer_group_name',
    RDR1.ItemCode AS 'pdt_code',
    OITM.ItemName AS 'pdt_name',
    COALESCE (RDR1.unitMsr, RDR1.unitMsr2) AS 'uom',
    ORDR.SlpCode AS 'sales_employee_code',
    OSLP.SlpName AS 'sales_employee',
    RDR1.Quantity AS 'qty',
    RDR1.Price AS 'price',
    RDR1.Quantity * RDR1.Price AS 'amount'
FROM
    RDR1
    LEFT JOIN ORDR ON RDR1.DocEntry = ORDR.DocEntry
    LEFT JOIN OITM ON RDR1.ItemCode = OITM.ItemCode
    LEFT JOIN OCRD ON ORDR.CardCode = OCRD.CardCode
    LEFT JOIN OSLP ON ORDR.SlpCode = OSLP.SlpCode
    LEFT JOIN OCRG ON OCRD.GroupCode = OCRG.GroupCode
WHERE
    RDR1.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND ORDR.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND ORDR.Canceled = 'N'
    AND RDR1.ItemCode IS NOT NULL;