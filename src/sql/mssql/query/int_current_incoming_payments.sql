SELECT
    DATEFROMPARTS(YEAR(ORCT.DocDate), MONTH(ORCT.DocDate), 1) AS 'agg_date',
    DocDate AS 'doc_date',
    DocNum AS 'doc_num',
    CASE
        WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
        ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
    END AS 'customer_group_name',
    ORCT.CardCode AS 'customer_code',
    OCRD.CardName AS 'customer_name',
    OSLP.SlpName AS 'sales_employee_name',
    DocTotal AS 'amount'
FROM
    ORCT
    LEFT JOIN OCRD ON ORCT.CardCode = OCRD.CardCode
    LEFT JOIN OSLP ON OCRD.SlpCode = OSLP.SlpCode
WHERE
    ORCT.Canceled != 'N'
    AND ORCT.DocDate BETWEEN {{start_date}} AND {{end_date}};