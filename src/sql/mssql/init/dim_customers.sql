WITH all_customers AS (
    SELECT
        CardCode,
        CardName,
        CmpPrivate,
        [Address],
        ZipCode,
        OOND.IndDesc,
        OOND.IndName,
        OCRG.GroupName,
        OCTG.PymntGroup,
        ValidFor
    FROM
        OCRD
        LEFT JOIN OOND ON OCRD.IndustryC = OOND.IndCode
        LEFT JOIN OCRG ON OCRD.GroupCode = OCRG.GroupCode
        LEFT JOIN OCTG ON OCRD.GroupNum = OCTG.GroupNum
    WHERE
        CardType = 'C'
),
active_customers AS (
    SELECT
        DISTINCT CardCode,
        1 AS 'is_active'
    FROM
        ORDR
    WHERE
        DocDate >= {{cutoff_date}}
)
SELECT
    all_customers.CardCode AS 'customer_code',
    CardName AS 'name',
    CmpPrivate AS 'entity_type',
    [Address] AS 'address',
    ZipCode AS 'zipcode',
    IndDesc AS 'industry',
    GroupName AS 'trade_ind',
    PymntGroup AS 'payment_terms',
    COALESCE(is_active, 0) AS 'is_active'
FROM
    all_customers
    LEFT JOIN active_customers ON all_customers.CardCode = active_customers.CardCode
WHERE
    CardName IS NOT NULL
    AND ValidFor = 'Y';