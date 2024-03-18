WITH active_suppliers AS (
    SELECT
        DISTINCT CardCode
    FROM
        OPCH
    WHERE
        OPCH.DocDate BETWEEN {{start_date}} AND {{end_date}}
        AND Canceled != 'Y'
),
all_suppliers AS (
    SELECT DISTINCT CardCode FROM OPCH
)
SELECT
    all_suppliers.CardCode AS 'customer_code',
    CASE
        WHEN active_suppliers.CardCode IS NULL THEN 0
        ELSE 1
    END AS 'is_active'
FROM
    all_suppliers
    LEFT JOIN active_suppliers ON all_suppliers.CardCode = active_suppliers.CardCodes