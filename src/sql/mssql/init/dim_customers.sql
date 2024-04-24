WITH dim_customers AS (
    SELECT
        CardCode AS 'customer_code',
        CardName AS 'name',
        CmpPrivate AS 'entity_type',
        CASE
            WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
            ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
        END AS 'customer_group_name',
        OSLP.SlpName AS 'sales_employee',
        CONCAT(COALESCE(Address,''), ' ', COALESCE(Block,''), ' ', COALESCE(Building,''), ' ' , COALESCE(ZipCode,'')) AS 'address',
        ZipCode AS 'zipcode',
        OOND.IndName AS 'industry',
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
),
first_latest_sales_date AS (
    SELECT
        CardCode 'customer_code',
        MIN(DocDate) AS 'first_sales_date',
        MAX(DocDate) AS 'latest_sales_date',
        CONCAT(
            ROUND(DATEDIFF(day, MIN(DocDate), GETDATE()) / 365, 0),
            ' Years',
            ' & ',
            DATEDIFF(day, MIN(DocDate), GETDATE()) % 365,
            ' Days'
        ) AS 'relationship_length'
    FROM
        ORDR
    GROUP BY
        CardCode
)
SELECT
    dim_customers.*,
    first_sales_date,
    latest_sales_date,
    relationship_length
FROM
    dim_customers
    LEFT JOIN first_latest_sales_date ON dim_customers.customer_code = first_latest_sales_date.customer_code;