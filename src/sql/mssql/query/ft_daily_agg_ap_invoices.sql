SELECT
    DATEFROMPARTS(YEAR(OPCH.DocDate), MONTH(OPCH.DocDate), 1) AS 'start_of_month',
    OPCH.DocDate AS 'doc_date',
    OPCH.CardCode AS 'vendor_code',
    OPCH.SlpCode AS 'sales_employee_code',
    SUM(OPCH.DocTotal) AS 'amount',
    SUM(OPCH.PaidToDate) AS 'paid_amount',
    SUM(OPCH.DocTotal) - SUM(OPCH.PaidToDate) AS 'outstanding_amount'
FROM
    OPCH
WHERE
    OPCH.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND OPCH.CANCELED = 'N'
GROUP BY DocDate, CardCode, SlpCode;