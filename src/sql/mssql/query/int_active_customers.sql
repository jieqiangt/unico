SELECT
    CardCode AS 'customer_code',
    CardName AS 'customer_name'
FROM
    OCRD
    LEFT JOIN OSLP ON OCRD.SlpCode = OSLP.SlpCode
WHERE
    CardType = 'C'
    AND validFor = 'Y'
    AND CardName IS NOT NULL;