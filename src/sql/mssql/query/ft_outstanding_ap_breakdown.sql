WITH accounts_payable AS (
    SELECT
        DATEFROMPARTS(YEAR(OPCH.DocDate), MONTH(OPCH.DocDate), 1) AS 'agg_date',
        OPCH.CardCode AS 'vendor_code',
        OCRD.CardName AS 'vendor_name',
        OCRG.GroupName AS 'vendor_type',
        OPCH.SlpCode AS 'sales_employee_code',
        OSLP.SlpName AS 'sales_employee_name',
        SUM(OPCH.DocTotal) - SUM(PaidToDate) AS 'amount_with_tax'
    FROM
        OPCH
        LEFT JOIN OCRD ON OPCH.CardCode = OCRD.CardCode
        LEFT JOIN OCRG ON OCRD.GroupCode = OCRG.GroupCode
        LEFT JOIN OSLP ON OPCH.SlpCode = OSLP.SlpCode
    WHERE
        OPCH.DocDate BETWEEN {{start_date}} AND {{end_date}}
        AND OPCH.CANCELED = 'N'
    GROUP BY
        DATEFROMPARTS(YEAR(OPCH.DocDate), MONTH(OPCH.DocDate), 1),
        OPCH.CardCode,
        OCRD.CardName,
        OCRG.GroupName,
        OPCH.SlpCode,
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
    vendor_code,
    vendor_name,
    vendor_type,
    sales_employee_code,
    sales_employee_name,
    amount_with_tax
FROM
    accounts_payable;