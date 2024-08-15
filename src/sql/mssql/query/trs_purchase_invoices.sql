SELECT
    TRIM(CONVERT(char, PCH1.DocDate, 112)) AS 'doc_date',
	DATEFROMPARTS(YEAR(PCH1.DocDate), MONTH(PCH1.DocDate), 1) AS 'start_of_month',
    OPCH.DocNum AS 'doc_num',
	PCH1.LineNum AS 'line_num',
    OPCH.CardCode AS 'supplier_code',
    PCH1.ItemCode AS 'pdt_code',
    PCH1.Quantity AS 'qty',
    PCH1.Price AS 'price',
    PCH1.LineTotal AS 'amount'
FROM
    PCH1
    LEFT JOIN OPCH ON PCH1.DocEntry = OPCH.DocEntry
    WHERE OPCH.CANCELED = 'N'
    AND DocType = 'I'
	AND OPCH.DocDate BETWEEN {{start_date}} AND {{end_date}}
	AND PCH1.DocDate BETWEEN {{start_date}} AND {{end_date}};