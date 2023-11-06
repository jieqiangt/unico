SELECT
    TRIM(CONVERT(char, ORIN.DocDate, 112)) AS 'doc_date',
    ORIN.SlpCode AS 'sales_employee_code',
    OSLP.SlpName AS 'sales_employee',
    DocTotal AS 'amount'
FROM
    ORIN
    LEFT JOIN OSLP ON ORIN.SlpCode = OSLP.SlpCode
WHERE
    DocDate BETWEEN {{start_date}} AND {{end_date}};