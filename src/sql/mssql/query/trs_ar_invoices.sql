SELECT
    DATEFROMPARTS(
        YEAR(DocDate),
        MONTH(DocDate),
        DAY(DocDate)
    ) AS 'doc_date',
    DATEFROMPARTS(YEAR(OINV.DocDate), MONTH(OINV.DocDate), 1) AS 'start_of_month',
    OINV.DocNum AS 'doc_num',
    OINV.CardCode AS 'customer_code',
    OINV.SlpCode AS 'sales_employee_code',
    OINV.DocTotal AS 'amount',
    OINV.PaidToDate AS 'paid_amount',
    (OINV.DocTotal - OINV.PaidToDate) AS 'outstanding_amount'
FROM
    OINV
WHERE
    OINV.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND OINV.CANCELED = 'N';