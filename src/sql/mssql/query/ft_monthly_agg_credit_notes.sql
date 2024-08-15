SELECT
    DATEFROMPARTS(YEAR(RIN1.DocDate), MONTH(RIN1.DocDate), 1) AS 'start_of_month',
    ORIN.SlpCode AS 'sales_employee_code',
    ORIN.CardCode AS 'customer_code',
    RIN1.ItemCode AS 'pdt_code',
    SUM(RIN1.Quantity) AS 'qty',
    AVG(RIN1.Price) AS 'price',
    SUM(RIN1.LineTotal) AS 'amount'
FROM
    RIN1
    LEFT JOIN ORIN ON RIN1.DocEntry = ORIN.DocEntry
WHERE
    RIN1.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND ORIN.Canceled = 'N'
GROUP BY DATEFROMPARTS(YEAR(RIN1.DocDate), MONTH(RIN1.DocDate), 1), ORIN.SlpCode, ORIN.CardCode, RIN1.ItemCode;