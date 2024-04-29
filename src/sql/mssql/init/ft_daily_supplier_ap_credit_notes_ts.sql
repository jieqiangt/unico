SELECT
    DATEFROMPARTS(
        YEAR(RPC1.DocDate),
        MONTH(RPC1.DocDate),
        1
    ) AS 'start_of_month_date',
    RPC1.DocDate AS 'as_of_date',
    ORPC.CardCode AS 'supplier_code',
    OCRD.CardName AS 'supplier_name',
    'Credit Note' AS 'value_type',
    SUM(LineTotal) AS 'value'
FROM
    RPC1
    LEFT JOIN ORPC ON RPC1.DocEntry = ORPC.DocEntry
    LEFT JOIN OCRD ON ORPC.CardCode = OCRD.CardCode
WHERE
    RPC1.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND ORPC.Canceled = 'N'
    AND RPC1.Price > 0.01
    AND CHARINDEX('ZS', RPC1.ItemCode) = 0
GROUP BY
    RPC1.DocDate,
    CASE
        WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
        ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
    END,
    ORPC.CardCode,
    OCRD.CardName
UNION
SELECT
    DATEFROMPARTS(YEAR(OPCH.DocDate), MONTH(OPCH.DocDate), 1) AS 'start_of_month_date',
    OPCH.DocDate AS 'as_of_date',
    OPCH.CardCode AS 'supplier_code',
    OCRD.CardName AS 'supplier_name',
    'Outstanding AP' AS 'value_type',
    SUM(OPCH.DocTotal - OPCH.PaidToDate) AS 'value'
FROM
    OPCH
    LEFT JOIN OCRD ON OPCH.CardCode = OCRD.CardCode
WHERE
    OPCH.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND OPCH.CANCELED = 'N'
GROUP BY
    OPCH.DocDate,
    OPCH.CardCode,
    OCRD.CardName 
UNION
SELECT
    DATEFROMPARTS(YEAR(OPCH.DocDate), MONTH(OPCH.DocDate), 1) AS 'start_of_month_date',
    OPCH.DocDate AS 'as_of_date',
    OPCH.CardCode AS 'supplier_code',
    OCRD.CardName AS 'supplier_name',
    'Paid AP' AS 'value_type',
    SUM(OPCH.PaidToDate) AS 'value'
FROM
    OPCH
    LEFT JOIN OCRD ON OPCH.CardCode = OCRD.CardCode
WHERE
    OPCH.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND OPCH.CANCELED = 'N'
GROUP BY
    OPCH.DocDate,
    CASE
        WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
        ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
    END,
    OPCH.CardCode,
    OCRD.CardName
;   
