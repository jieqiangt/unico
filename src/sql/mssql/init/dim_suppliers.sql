SELECT
    CardCode AS 'supplier_code',
    CardName AS 'name',
    CmpPrivate AS 'entity_type',
    [Address] AS 'address',
    ZipCode AS 'zipcode',
    OOND.IndDesc AS 'overseas_local_ind',
    OCRG.GroupName AS 'trade_ind',
    OCTG.PymntGroup AS 'payment_terms',
    ValidFor AS 'is_active'
FROM
    OCRD
    LEFT JOIN OOND ON OCRD.IndustryC = OOND.IndCode
    LEFT JOIN OCRG ON OCRD.GroupCode = OCRG.GroupCode
    LEFT JOIN OCTG ON OCRD.GroupNum = OCTG.GroupNum
WHERE
    CardType = 'S'
    AND CardName IS NOT NULL