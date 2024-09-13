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
        RDR1.DocDate BETWEEN '2024-06-01'
        AND '2024-07-24'
        AND ORDR.DocDate BETWEEN '2024-06-01'
        AND '2024-07-24'
        AND ORDR.Canceled = 'N'
        AND RDR1.ItemCode IS NOT NULL
),
unique_sales_employee AS (
    SELECT
        DISTINCT pdt_code,
        sales_employee
    FROM
        orders
),
sales_employee_list AS (
    SELECT
        pdt_code,
        STRING_AGG(CONVERT(NVARCHAR(max), sales_employee), ',') as sales_employee_list
    FROM
        unique_sales_employee
    GROUP BY
        pdt_code
)
SELECT
    *
FROM
    sales_employee_list;