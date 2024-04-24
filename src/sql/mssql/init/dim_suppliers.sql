WITH dim_suppliers AS (
    SELECT
        CardCode AS 'supplier_code',
        CardName AS 'name',
        CmpPrivate AS 'entity_type',
        CONCAT(COALESCE(Address,''), ' ', COALESCE(Block,''), ' ', COALESCE(Building,''), ' ' , COALESCE(ZipCode,'')) AS 'address',
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
),
first_last_purchase_date AS (
    SELECT
        CardCode AS 'supplier_code',
        MIN(DocDate) AS 'first_purchase_date',
        MAX(DocDate) AS 'latest_purchase_date',
        CONCAT(
            ROUND(DATEDIFF(day, MIN(DocDate), GETDATE()) / 365, 0),
            ' Years',
            ' & ',
            DATEDIFF(day, MIN(DocDate), GETDATE()) % 365,
            ' Days'
        ) AS 'relationship_length'
    FROM
        OPCH
    GROUP BY
        CardCode
)
SELECT
    dim_suppliers.*,
    first_purchase_date,
    latest_purchase_date,
    relationship_length
FROM
    dim_suppliers
    LEFT JOIN first_last_purchase_date ON dim_suppliers.supplier_code = first_sales_date.supplier_code;