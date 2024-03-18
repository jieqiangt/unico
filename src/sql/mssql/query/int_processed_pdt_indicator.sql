SELECT
    ItemCode AS 'pdt_code',
    CASE
        WHEN QryGroup2 = 'Y' THEN 1
        ELSE 0
    END AS 'processed_pdt_ind'
FROM
    OITM