WITH all_suppliers AS (
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
        CardType = 'S'
),
active_suppliers AS (
    SELECT
        DISTINCT CardCode,
        1 AS 'is_active'
    FROM
        OPCH
    WHERE
        DocDate >= {{cutoff_date}}
)
SELECT
    all_suppliers.CardCode AS 'supplier_code',
    CardName AS 'name',
    CmpPrivate AS 'entity_type',
    [Address] AS 'address',
    ZipCode AS 'zipcode',
    IndDesc AS 'overseas_local_ind',
    GroupName AS 'trade_ind',
    PymntGroup AS 'payment_terms',
    COALESCE(is_active, 0) AS 'is_active'
FROM
    all_suppliers
    LEFT JOIN active_suppliers ON all_suppliers.CardCode = active_suppliers.CardCode
WHERE 
    CardName IS NOT NULL
	AND validFor = 'Y'