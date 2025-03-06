WITH sales AS (
    SELECT
        OINV.DocDate AS 'doc_date',
        OINV.CardCode AS 'customer_code',
        OCRD.CardName as 'customer_name',
        OCTG.PymntGroup AS 'payment_terms',
        OSLP.SlpName AS 'sales_employee',
        CASE
            WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
            ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
        END AS 'customer_group_name',
        OINV.DocTotal AS 'amount'
    FROM
        OINV
        LEFT JOIN OCRD ON OINV.CardCode = OCRD.CardCode
        LEFT JOIN OCTG ON OCRD.GroupNum = OCTG.GroupNum
        LEFT JOIN OSLP ON OCRD.SlpCode = OSLP.SlpCode
    WHERE
        OINV.DocDate BETWEEN {{start_date}} AND {{end_date}}
    UNION
    SELECT
        ORIN.DocDate AS 'doc_date',
        ORIN.CardCode AS 'customer_code',
        OCRD.CardName as 'customer_name',
        OCTG.PymntGroup AS 'payment_terms',
        OSLP.SlpName AS 'sales_employee',
        CASE
            WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
            ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
        END AS 'customer_group_name',
        ORIN.DocTotal AS 'amount'
    FROM
        ORIN
        LEFT JOIN OCRD ON ORIN.CardCode = OCRD.CardCode
        LEFT JOIN OCTG ON OCRD.GroupNum = OCTG.GroupNum
        LEFT JOIN OSLP ON OCRD.SlpCode = OSLP.SlpCode
    WHERE
        ORIN.DocDate BETWEEN {{start_date}} AND {{end_date}}
)
SELECT
    doc_date,
    customer_code,
    customer_name,
    customer_group_name,
    sales_employee,
    payment_terms,
    SUM(amount) AS 'amount'
FROM
    sales
GROUP BY
    doc_date,
    customer_code,
    customer_name,
    customer_group_name,
    sales_employee,
    payment_terms;