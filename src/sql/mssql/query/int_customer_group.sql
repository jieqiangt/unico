SELECT
    OCRD.CardCode AS 'customer_code',
    CASE
        WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
        ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
    END AS 'customer_group_name'
FROM
    OCRD
    LEFT JOIN OCRG ON OCRD.GroupCode = OCRG.GroupCode