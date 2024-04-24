SELECT
    DATEFROMPARTS(
        YEAR(RIN1.DocDate),
        MONTH(RIN1.DocDate),
        1
    ) AS 'start_of_month_date',
    RIN1.DocDate AS 'as_of_date',
    CASE
        WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
        ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
    END AS 'customer_group_name',
    ORIN.CardCode AS 'customer_code',
    OCRD.CardName AS 'customer_name',
    'Credit Note' AS 'value_type',
    SUM(LineTotal) AS 'value'
FROM
    RIN1
    LEFT JOIN ORIN ON RIN1.DocEntry = ORIN.DocEntry
    LEFT JOIN OCRD ON ORIN.CardCode = OCRD.CardCode
WHERE
    RIN1.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND ORIN.Canceled = 'N'
    AND RIN1.Price > 0.01
    AND CHARINDEX('ZS', RIN1.ItemCode) = 0
GROUP BY
    RIN1.DocDate,
    CASE
        WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
        ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
    END,
    ORIN.CardCode,
    OCRD.CardName
UNION
SELECT
    DATEFROMPARTS(YEAR(OINV.DocDate), MONTH(OINV.DocDate), 1) AS 'start_of_month_date',
    OINV.DocDate AS 'as_of_date',
    CASE
        WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
        ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
    END AS 'customer_group_name',
    OINV.CardCode AS 'customer_code',
    OCRD.CardName AS 'customer_name',
    'Outstanding AR' AS 'value_type',
    SUM(OINV.DocTotal - OINV.PaidToDate) AS 'value'
FROM
    OINV
    LEFT JOIN OCRD ON OINV.CardCode = OCRD.CardCode
WHERE
    OINV.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND OINV.CANCELED = 'N'
GROUP BY
    OINV.DocDate,
    CASE
        WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
        ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
    END,
    OINV.CardCode,
    OCRD.CardName 
UNION
SELECT
    DATEFROMPARTS(YEAR(OINV.DocDate), MONTH(OINV.DocDate), 1) AS 'start_of_month_date',
    OINV.DocDate AS 'as_of_date',
    CASE
        WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
        ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
    END AS 'customer_group_name',
    OINV.CardCode AS 'customer_code',
    OCRD.CardName AS 'customer_name',
    'Paid AR' AS 'value_type',
    SUM(OINV.PaidToDate) AS 'value'
FROM
    OINV
    LEFT JOIN OCRD ON OINV.CardCode = OCRD.CardCode
WHERE
    OINV.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND OINV.CANCELED = 'N'
GROUP BY
    OINV.DocDate,
    CASE
        WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
        ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
    END,
    OINV.CardCode,
    OCRD.CardName
;   
