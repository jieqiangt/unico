WITH orders AS (
    SELECT
        MONTH(ORDR.DocDate) AS 'sales_month',
        ORDR.CardCode AS 'customer_code',
        OCRD.CardName AS 'customer_name',
        CASE
            WHEN OCRD.U_AF_CUSTGROUP = '' THEN OCRD.CardName
            ELSE COALESCE (OCRD.U_AF_CUSTGROUP, OCRD.CardName)
        END AS 'customer_group_name',
        OCTG.PymntGroup AS 'payment_terms',
        RDR1.ItemCode AS 'pdt_code',
        OITM.ItemName AS 'pdt_name',
        COALESCE (RDR1.unitMsr, RDR1.unitMsr2) AS 'uom',
        OSLP.SlpName AS 'sales_employee',
        RDR1.LineTotal AS 'amount'
    FROM
        RDR1
        LEFT JOIN ORDR ON RDR1.DocEntry = ORDR.DocEntry
        LEFT JOIN OITM ON RDR1.ItemCode = OITM.ItemCode
        LEFT JOIN OCRD ON ORDR.CardCode = OCRD.CardCode
        LEFT JOIN OSLP ON ORDR.SlpCode = OSLP.SlpCode
        LEFT JOIN OCTG ON OCRD.GroupNum = OCTG.GroupNum
    WHERE
        RDR1.DocDate BETWEEN {{start_date}}AND {{end_date}}
        AND ORDR.DocDate BETWEEN {{start_date}} AND {{end_date}}
        AND ORDR.Canceled = 'N'
        AND RDR1.ItemCode IS NOT NULL
),
agg_amt AS (
    SELECT
        pdt_code,
        COALESCE([1],0) AS [Jan],
        COALESCE([2],0)AS [Feb],
        COALESCE([3],0) AS [Mar],
        COALESCE([4],0) AS [Apr],
        COALESCE([5],0) AS [May],
        COALESCE([6],0) AS [Jun],
        COALESCE([7],0) AS [Jul],
        COALESCE([8],0) AS [Aug],
        COALESCE([9],0) AS [Sep],
        COALESCE([10],0) AS [Oct],
        COALESCE([11],0) AS [Nov],
        COALESCE([12],0) AS [Dec]
    FROM
        (SELECT sales_month,pdt_code,amount FROM orders) AS S PIVOT (
            SUM(amount) FOR sales_month IN (
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
        ) AS pivoted_sales
)
SELECT * FROM agg_amt;