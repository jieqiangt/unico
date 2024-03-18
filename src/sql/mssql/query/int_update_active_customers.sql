WITH active_customers AS (
    SELECT
        DISTINCT CardCode
    FROM
        ORDR
    WHERE
        ORDR.DocDate BETWEEN {{start_date}} AND {{end_date}}
        AND Canceled != 'Y'
),
all_customers AS (
    SELECT DISTINCT CardCode FROM OCRD
)
SELECT
    all_customers.CardCode AS 'customer_code',
    CASE
        WHEN active_customers.CardCode IS NULL THEN 0
        ELSE 1
    END AS 'is_active'
FROM
    all_customers
    LEFT JOIN active_customers ON all_customers.CardCode = active_customers.CardCode