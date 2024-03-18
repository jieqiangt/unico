WITH orders AS (
    SELECT
        ORDR.DocDate AS 'doc_date',
        DATEFROMPARTS(YEAR(ORDR.DocDate), MONTH(ORDR.DocDate), 1) AS 'agg_date',
        ORDR.DocNum AS 'doc_num',
        RDR1.LineNum AS 'line_num',
        CASE
            WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
            ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
        END AS 'customer_group_name',
        ORDR.CardCode AS 'customer_code',
        OCRD.CardName AS 'customer_name',
        RDR1.ItemCode AS 'pdt_code',
        OITM.ItemName AS 'pdt_name',
        RDR1.SlpCode AS 'sales_employee_code',
        OSLP.SlpName AS 'sales_employee',
        COALESCE(RDR1.unitMsr, RDR1.unitMsr2) AS 'uom',
        RDR1.Price AS 'price',
        RDR1.Quantity AS 'qty',
        RDR1.Price * RDR1.Quantity AS 'amount',
        OITM.LastPurPrc AS 'last_purchase_price',
        OITM.LastPurDat AS 'last_purchase_date'
    FROM
        ORDR
        LEFT JOIN OCRD ON ORDR.CardCode = OCRD.CardCode
        LEFT JOIN RDR1 ON ORDR.DocEntry = RDR1.DocEntry
        LEFT JOIN OITM ON RDR1.ItemCode = OITM.ItemCode
        LEFT JOIN OSLP ON RDR1.SlpCode = OSLP.SlpCode
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
        ) AS 'std_in_price',
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
    doc_date,
    agg_date,
    doc_num,
    line_num,
    orders.customer_group_name,
    customer_code,
    customer_name,
    orders.pdt_code,
    pdt_name,
    sales_employee_code,
    sales_employee,
    uom,
    price,
    qty,
    amount,
    std_in_price,
    distinct_customers_count,
    last_purchase_price,
    last_purchase_date,
    (price - last_purchase_price) / price AS 'profit_margin'
FROM
    orders
    INNER JOIN flagged_pdts ON orders.customer_group_name = flagged_pdts.customer_group_name
    AND orders.pdt_code = flagged_pdts.pdt_code;