WITH pivoted_qty AS (
    SELECT
        pdt_code,
        pdt_name,
        customer_code,
        customer_name,
        customer_group_name,
        uom,
        COALESCE([1], 0) AS [jan],
        COALESCE([2], 0) AS [feb],
        COALESCE([3], 0) AS [mar],
        COALESCE([4], 0) AS [apr],
        COALESCE([5], 0) AS [may],
        COALESCE([6], 0) AS [jun],
        COALESCE([7], 0) AS [jul],
        COALESCE([8], 0) AS [aug],
        COALESCE([9], 0) AS [sep],
        COALESCE([10], 0) AS [oct],
        COALESCE([11], 0) AS [nov],
        COALESCE([12], 0) AS [december]
    FROM
        (
            SELECT
                MONTH(T0.DocDate) AS 'sales_month',
                T0.ItemCode AS 'pdt_code',
                T2.ItemName AS 'pdt_name',
                T3.CardCode AS 'customer_code',
                T3.CardName AS 'customer_name',
                CASE
                    WHEN T3.U_AF_CUSTGROUP = '' THEN T3.CardName
                    ELSE COALESCE (T3.U_AF_CUSTGROUP, T3.CardName)
                END AS 'customer_group_name',
                COALESCE (T0.unitMsr, T0.unitMsr2) AS 'uom',
                T0.Quantity AS 'qty'
            FROM
                RDR1 T0
                LEFT JOIN ORDR T1 ON T0.DocEntry = T1.DocEntry
                LEFT JOIN OITM T2 ON T0.ItemCode = T2.ItemCode
                LEFT JOIN OCRD T3 ON T1.CardCode = T3.CardCode
            WHERE
                T0.DocDate >= {{start_date}}
                AND T0.DocDate <= {{end_date}}
                AND T1.DocDate >= {{start_date}}
                AND T1.DocDate <= {{end_date}}
                AND T1.Canceled = 'N'
                AND T0.ItemCode IS NOT NULL
        ) AS sales_orders PIVOT (
            SUM(qty) FOR [sales_month] IN (
                [1],
                [2],
                [3],
                [4],
                [5],
                [6],
                [7],
                [8],
                [9],
                [10],
                [11],
                [12]
            )
        ) AS pivoted_qty
),
current_inv AS (
    SELECT
        ItemCode AS 'pdt_code',
        SUM(OnHand) + SUM(IsCommited) AS 'current_inv'
    FROM
        OITW
    GROUP BY
        ItemCode
)
SELECT
    pivoted_qty.pdt_code,
    pdt_name,
    current_inv,
    customer_code,
    customer_name,
    customer_group_name,
    uom,
    [jan],
    [feb],
    [mar],
    [apr],
    [may],
    [jun],
    [jul],
    [aug],
    [sep],
    [oct],
    [nov],
    [december]
FROM
    pivoted_qty
    LEFT JOIN current_inv ON pivoted_qty.pdt_code = current_inv.pdt_code;