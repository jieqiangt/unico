SELECT
    TRIM(CONVERT(char, RIN1.DocDate, 112)) AS 'doc_date',
    DATEFROMPARTS(YEAR(RIN1.DocDate), MONTH(RIN1.DocDate), 1) AS 'agg_date',
    ORIN.DocNum AS 'doc_num',
    RIN1.LineNum AS 'line_num',
    ORIN.SlpCode AS 'sales_employee_code',
    OSLP.SlpName AS 'sales_employee',
    ORIN.CardCode AS 'customer_code',
    OCRD.CardCode AS 'customer_name',
    CASE
        WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
        ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
    END AS 'customer_group_name',
    RIN1.ItemCode AS 'pdt_code',
    OITM.ItemName AS 'pdt_name',
    OITM.SalUnitMsr AS 'uom',
    RIN1.Quantity AS 'qty',
    RIN1.Price AS 'price',
    RIN1.Quantity * RIN1.Price AS 'amount'
FROM
    RIN1
    LEFT JOIN ORIN ON RIN1.DocEntry = ORIN.DocEntry
    LEFT JOIN OITM ON RIN1.ItemCode = OITM.ItemCode
    LEFT JOIN OSLP ON ORIN.SlpCode = OSLP.SlpCode
    LEFT JOIN OCRD ON ORIN.CardCode = OCRD.CardCode
WHERE
    RIN1.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND ORIN.Canceled = 'N';