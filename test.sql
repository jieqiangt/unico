WITH pivoted_qty AS (
    SELECT
        pdt_code,
        pdt_name,
        uom,
        COALESCE([1], 0) AS [Jan],
        COALESCE([2], 0) AS [Feb],
        COALESCE([3], 0) AS [Mar],
        COALESCE([4], 0) AS [Apr],
        COALESCE([5], 0) AS [May],
        COALESCE([6], 0) AS [Jun],
        COALESCE([7], 0) AS [Jul],
        COALESCE([8], 0) AS [Aug],
        COALESCE([9], 0) AS [Sep],
        COALESCE([10], 0) AS [Oct],
        COALESCE([11], 0) AS [Nov],
        COALESCE([12], 0) AS [Dec]
    FROM
        (
            SELECT
                MONTH(T0.DocDate) AS 'sales_month',
                T0.ItemCode AS 'pdt_code',
                T2.ItemName AS 'pdt_name',
                COALESCE (T0.unitMsr, T0.unitMsr2) AS 'uom',
                T0.Quantity AS 'qty'
            FROM
                RDR1 T0
                LEFT JOIN ORDR T1 ON T0.DocEntry = T1.DocEntry
                LEFT JOIN OITM T2 ON T0.ItemCode = T2.ItemCode
            WHERE
                T0.DocDate >=[%0]
                AND T0.DocDate <= [%1]
                AND T1.DocDate >=[%0]
                AND T1.DocDate <= [%1]
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
    uom,
    [Jan],
    [Feb],
    [Mar],
    [Apr],
    [May],
    [Jun],
    [Jul],
    [Aug],
    [Sep],
    [Oct],
    [Nov],
    [Dec]
FROM
    pivoted_qty
    LEFT JOIN current_inv ON pivoted_qty.pdt_code = current_inv.pdt_code