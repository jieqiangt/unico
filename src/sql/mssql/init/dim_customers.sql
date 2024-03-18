WITH all_customers AS (
    SELECT
        CardCode,
        CardName,
        CmpPrivate,
        CASE
        WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
        ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
        END AS 'customer_group_name',
        OSLP.SlpName,
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
        LEFT JOIN OSLP ON OCRD.SlpCode = OSLP.SlpCode
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
    customer_group_name,
    SlpName AS 'sales_employee',
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