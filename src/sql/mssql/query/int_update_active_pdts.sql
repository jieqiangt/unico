WITH active_pdts AS (
    SELECT
        DISTINCT ItemCode
    FROM
        RDR1
        LEFT JOIN ORDR ON RDR1.DocEntry = ORDR.DocEntry
    WHERE
        RDR1.DocDate BETWEEN {{start_date}} AND {{end_daten}}
        AND Canceled != 'Y'
),
all_pdts AS (
    SELECT
        DISTINCT ItemCode
    FROM
        OITM
)
SELECT
    all_pdts.ItemCode AS 'pdt_code',
    CASE
        WHEN active_pdts.ItemCode IS NULL THEN 0
        ELSE 1
    END AS 'is_active'
FROM
    all_pdts
    LEFT JOIN active_pdts ON all_pdts.ItemCode = active_pdts.ItemCode