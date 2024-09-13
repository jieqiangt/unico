SELECT
    DATEFROMPARTS(YEAR(INV1.DocDate), MONTH(INV1.DocDate), 1) AS 'start_of_month',
    OINV.CardCode AS 'customer_code',
    INV1.ItemCode AS 'pdt_code',
    OINV.SlpCode AS 'sales_employee_code',
    SUM(INV1.Quantity) AS 'qty',
    AVG(INV1.Price) AS 'price',
    SUM(INV1.LineTotal) AS 'amount'
FROM
    INV1
    LEFT JOIN OINV ON INV1.DocEntry = OINV.DocEntry
WHERE
    INV1.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND OINV.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND OINV.Canceled = 'N'
    AND INV1.ItemCode IS NOT NULL
GROUP BY
    DATEFROMPARTS(YEAR(INV1.DocDate), MONTH(INV1.DocDate), 1),
    OINV.CardCode,
    INV1.ItemCode,
    OINV.SlpCode;