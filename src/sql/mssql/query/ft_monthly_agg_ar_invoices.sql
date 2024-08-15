SELECT
    DATEFROMPARTS(YEAR(OINV.DocDate), MONTH(OINV.DocDate), 1) AS 'start_of_month',
    OINV.CardCode AS 'customer_code',
    OINV.SlpCode AS 'sales_employee_code',
    SUM(OINV.DocTotal) AS 'amount',
    SUM(OINV.PaidToDate) AS 'paid_amount',
    SUM(OINV.DocTotal) - SUM(OINV.PaidToDate) AS 'outstanding_amount'
FROM
    OINV
WHERE
    OINV.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND OINV.CANCELED = 'N'
GROUP BY DATEFROMPARTS(YEAR(OINV.DocDate), MONTH(OINV.DocDate), 1), CardCode, SlpCode;