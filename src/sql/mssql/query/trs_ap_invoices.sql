SELECT
    DATEFROMPARTS(YEAR(OPCH.DocDate), MONTH(OPCH.DocDate), 1) AS 'start_of_month',
    OPCH.DocNum AS 'doc_num',
    OPCH.DocDate AS 'doc_date',
    OPCH.CardCode AS 'vendor_code',
    OPCH.SlpCode AS 'sales_employee_code',
    OPCH.DocTotal AS 'amount',
    OPCH.PaidToDate AS 'paid_amount',
    (OPCH.DocTotal - OPCH.PaidToDate) AS 'outstanding_amount'
FROM
    OPCH
    LEFT JOIN OCRD ON OPCH.CardCode = OCRD.CardCode
WHERE
    OPCH.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND OPCH.CANCELED = 'N';