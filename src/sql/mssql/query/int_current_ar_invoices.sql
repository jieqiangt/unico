SELECT
    DATEFROMPARTS(YEAR(OINV.DocDate), MONTH(OINV.DocDate), 1) AS 'agg_date',
    'accounts_receivable' AS 'doc_type',
    OINV.DocStatus AS 'doc_status',
    OINV.DocNum AS 'doc_num',
    OINV.DocDate AS 'doc_date',
    OINV.CardCode AS 'customer_code',
    OCRD.CardName AS 'customer_name',
    OCRG.GroupName AS 'customer_type',
    OINV.SlpCode AS 'sales_employee_code',
    OSLP.SlpName AS 'sales_employee_name',
    OINV.DocTotal AS 'amount_with_tax'
FROM
    OINV
    LEFT JOIN OCRD ON OINV.CardCode = OCRD.CardCode
    LEFT JOIN OCRG ON OCRD.GroupCode = OCRG.GroupCode
    LEFT JOIN OSLP ON OINV.SlpCode = OSLP.SlpCode
WHERE
    OINV.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND OINV.CANCELED = 'N'
ORDER BY
    DATEFROMPARTS(YEAR(OINV.DocDate), MONTH(OINV.DocDate), 1),
    OINV.DocNum;