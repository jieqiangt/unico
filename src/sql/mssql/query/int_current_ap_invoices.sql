SELECT
    DATEFROMPARTS(YEAR(OPCH.DocDate), MONTH(OPCH.DocDate), 1) AS 'agg_date',
    'accounts_payable' AS 'doc_type',
    OPCH.DocStatus AS 'doc_status',
    OPCH.DocNum AS 'doc_num',
    OPCH.DocDate AS 'doc_date',
    OPCH.CardCode AS 'vendor_code',
    OCRD.CardName AS 'vendor_name',
    OCRG.GroupName AS 'vendor_type',
    OPCH.SlpCode AS 'sales_employee_code',
    OSLP.SlpName AS 'sales_employee_name',
    OPCH.DocTotal AS 'amount_with_tax'
FROM
    OPCH
    LEFT JOIN OCRD ON OPCH.CardCode = OCRD.CardCode
    LEFT JOIN OCRG ON OCRD.GroupCode = OCRG.GroupCode
    LEFT JOIN OSLP ON OPCH.SlpCode = OSLP.SlpCode
WHERE
    OPCH.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND OPCH.CANCELED = 'N'
ORDER BY
    DATEFROMPARTS(YEAR(OPCH.DocDate), MONTH(OPCH.DocDate), 1),
    OPCH.DocNum;