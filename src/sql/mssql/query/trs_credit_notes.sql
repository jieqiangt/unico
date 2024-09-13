SELECT
    DATEFROMPARTS(
        YEAR(RIN1.DocDate),
        MONTH(RIN1.DocDate),
        DAY(RIN1.DocDate)
    ) AS 'doc_date',
    DATEFROMPARTS(YEAR(RIN1.DocDate), MONTH(RIN1.DocDate), 1) AS 'start_of_month',
    ORIN.DocNum AS 'doc_num',
    RIN1.LineNum AS 'line_num',
    ORIN.SlpCode AS 'sales_employee_code',
    ORIN.CardCode AS 'customer_code',
    RIN1.ItemCode AS 'pdt_code',
    RIN1.Quantity AS 'qty',
    RIN1.Price AS 'price',
    RIN1.LineTotal AS 'amount'
FROM
    RIN1
    LEFT JOIN ORIN ON RIN1.DocEntry = ORIN.DocEntry
WHERE
    RIN1.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND ORIN.Canceled = 'N';