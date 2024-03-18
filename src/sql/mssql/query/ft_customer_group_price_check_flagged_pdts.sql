WITH orders AS (
    SELECT
        ORDR.DocDate AS 'doc_date',
        DocNum AS 'doc_num',
        CASE
            WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
            ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
        END AS 'customer_group_name',
        ORDR.CardCode AS 'customer_code',
        OCRD.CardName AS 'customer_name',
        RDR1.ItemCode AS 'pdt_code',
        OITM.ItemName AS 'pdt_name',
        RDR1.Price AS 'price',
        RDR1.Quantity AS 'qty'
    FROM
        ORDR
        LEFT JOIN OCRD ON ORDR.CardCode = OCRD.CardCode
        LEFT JOIN RDR1 ON ORDR.DocEntry = RDR1.DocEntry
        LEFT JOIN OITM ON RDR1.ItemCode = OITM.ItemCode
    WHERE
        ORDR.CardCode IN (
            SELECT
                OCRD.CardCode
            FROM
                OCRD
            WHERE
                OCRD.U_AF_CUSTGROUP IS NOT NULL
                AND OCRD.U_AF_CUSTGROUP NOT LIKE ''
        )
        AND ORDR.DocDate >= {{start_date}}
        AND price > 0.01
),
flagged_pdts AS (
    SELECT
        customer_group_name,
        pdt_code,
        ROUND(
            CASE
                WHEN COUNT(price) = 1 THEN 0
                ELSE STDEVP(price)
            END,
            2
        ) AS 'std',
        COUNT(DISTINCT customer_code) AS 'distinct_customers_count'
    FROM
        orders
    GROUP BY
        customer_group_name,
        pdt_code
    HAVING
        ROUND(
            CASE
                WHEN COUNT(price) = 1 THEN 0
                ELSE STDEVP(price)
            END,
            1
        ) > 0
        AND COUNT(DISTINCT customer_code) > 1
)
SELECT
    {{start_date}} AS 'as_of_date',
    customer_group_name,
    pdt_code,
    ItemName AS 'pdt_name'
FROM
    flagged_pdts
    LEFT JOIN OITM ON flagged_pdts.pdt_code = OITM.ItemCode;