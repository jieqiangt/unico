SELECT
    DATEFROMPARTS(
        YEAR(INV1.DocDate),
        MONTH(INV1.DocDate),
        DAY(INV1.DocDate)
    ) AS 'doc_date',
	DATEFROMPARTS(YEAR(INV1.DocDate), MONTH(INV1.DocDate), 1) AS 'start_of_month',
    OINV.DocNum AS 'doc_num',
	INV1.LineNum AS 'line_num',
    OINV.CardCode AS 'customer_code',
    OINV.SlpCode AS 'sales_employee_code',
    INV1.ItemCode AS 'pdt_code',
    INV1.Quantity AS 'qty',
    INV1.Price AS 'price',
    INV1.LineTotal AS 'amount'
FROM
    INV1
    LEFT JOIN OINV ON INV1.DocEntry = OINV.DocEntry
    WHERE OINV.CANCELED = 'N'
    AND DocType = 'I'
	AND OINV.DocDate BETWEEN {{start_date}} AND {{end_date}}
	AND INV1.DocDate BETWEEN {{start_date}} AND {{end_date}};