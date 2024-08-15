SELECT
	DATEFROMPARTS(YEAR(PCH1.DocDate), MONTH(PCH1.DocDate), 1) AS 'start_of_month',
    OPCH.CardCode AS 'supplier_code',
    PCH1.ItemCode AS 'pdt_code',
    SUM(PCH1.Quantity) AS 'qty',
    AVG(PCH1.Price) AS 'price',
    SUM(PCH1.LineTotal) AS 'amount'
FROM
    PCH1
    LEFT JOIN OPCH ON PCH1.DocEntry = OPCH.DocEntry
    WHERE OPCH.CANCELED = 'N'
    AND DocType = 'I'
	AND OPCH.DocDate BETWEEN {{start_date}} AND {{end_date}}
	AND PCH1.DocDate BETWEEN {{start_date}} AND {{end_date}}
GROUP BY DATEFROMPARTS(YEAR(PCH1.DocDate), MONTH(PCH1.DocDate), 1), OPCH.CardCode, PCH1.ItemCode