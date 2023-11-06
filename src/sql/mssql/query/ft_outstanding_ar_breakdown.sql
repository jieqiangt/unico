WITH accounts_receivable AS (
    SELECT
        DATEFROMPARTS(YEAR(OINV.DocDate), MONTH(OINV.DocDate), 1) AS 'agg_date',
        OINV.CardCode AS 'customer_code',
        OCRD.CardName AS 'customer_name',
        COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName) AS 'customer_group_name',
        OCRG.GroupName AS 'customer_type',
        OINV.SlpCode AS 'sales_employee_code',
        OSLP.SlpName AS 'sales_employee_name',
        SUM(OINV.DocTotal) AS 'amount_with_tax'
    FROM
        OINV
        LEFT JOIN OCRD ON OINV.CardCode = OCRD.CardCode
        LEFT JOIN OCRG ON OCRD.GroupCode = OCRG.GroupCode
        LEFT JOIN OSLP ON OINV.SlpCode = OSLP.SlpCode
    WHERE
        OINV.DocDate BETWEEN {{start_date}} AND {{end_date}}
        AND OINV.CANCELED = 'N'
        AND OINV.DocStatus = 'O'
    GROUP BY
        DATEFROMPARTS(YEAR(OINV.DocDate), MONTH(OINV.DocDate), 1),
        OINV.CardCode,
        OCRD.CardName,
        COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName),
        OCRG.GroupName,
        OINV.SlpCode,
        OSLP.SlpName
)
SELECT
    agg_date,
    CASE
        WHEN DATEDIFF(month, agg_date, GETDATE()) = 0 THEN 'Current Month'
        WHEN DATEDIFF(month, agg_date, GETDATE()) = 1 THEN 'Last Month'
        WHEN DATEDIFF(month, agg_date, GETDATE()) = 2 THEN '2 Months Ago'
        WHEN DATEDIFF(month, agg_date, GETDATE()) = 3 THEN '3 Months Ago'
        ELSE '> 3 Months Ago'
    END AS 'owed_period',
    customer_code,
    customer_name,
    customer_group_name,
    customer_type,
    sales_employee_code,
    sales_employee_name,
    amount_with_tax
FROM
    accounts_receivable;