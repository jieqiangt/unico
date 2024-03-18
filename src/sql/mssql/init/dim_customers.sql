SELECT
    CardCode AS 'customer_code',
    CardName AS 'name',
    CmpPrivate AS 'entity_type',
    CASE
        WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
        ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
    END AS 'customer_group_name',
    OSLP.SlpName AS 'sales_employee',
    [Address] AS 'address',
    ZipCode AS 'zipcode',
    OOND.IndDesc AS 'industry',
    OCRG.GroupName AS 'trade_ind',
    OCTG.PymntGroup AS 'payment_terms',
    ValidFor AS 'is_active'
FROM
    OCRD
    LEFT JOIN OOND ON OCRD.IndustryC = OOND.IndCode
    LEFT JOIN OCRG ON OCRD.GroupCode = OCRG.GroupCode
    LEFT JOIN OCTG ON OCRD.GroupNum = OCTG.GroupNum
    LEFT JOIN OSLP ON OCRD.SlpCode = OSLP.SlpCode
WHERE
    CardType = 'C'
    AND CardName IS NOT NULL