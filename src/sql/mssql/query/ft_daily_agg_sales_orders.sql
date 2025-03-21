SELECT
    DATEFROMPARTS(
        YEAR(RDR1.DocDate),
        MONTH(RDR1.DocDate),
        DAY(RDR1.DocDate)
    ) AS 'doc_date',
    DATEFROMPARTS(YEAR(RDR1.DocDate), MONTH(RDR1.DocDate), 1) AS 'start_of_month',
    ORDR.CardCode AS 'customer_code',
    RDR1.ItemCode AS 'pdt_code',
    ORDR.SlpCode AS 'sales_employee_code',
    SUM(RDR1.Quantity) AS 'qty',
    AVG(RDR1.Price) AS 'price',
    SUM(RDR1.LineTotal) AS 'amount'
FROM
    RDR1
    LEFT JOIN ORDR ON RDR1.DocEntry = ORDR.DocEntry
WHERE
    RDR1.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND ORDR.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND ORDR.Canceled = 'N'
    AND RDR1.ItemCode IS NOT NULL
GROUP BY
    RDR1.DocDate,
    ORDR.CardCode,
    RDR1.ItemCode,
    ORDR.SlpCode;