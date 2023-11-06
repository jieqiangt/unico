SELECT
    TRIM(CONVERT(char, PCH1.DocDate, 112)) AS 'doc_date',
    PCH1.ItemCode AS 'pdt_code',
    PCH1.Price AS 'price',
    PCH1.Quantity AS 'qty'
FROM
    PCH1
    LEFT JOIN OPCH ON PCH1.DocEntry = OPCH.DocEntry
    WHERE OPCH.CANCELED = 'N'
    AND DocType = 'I'
    AND PCH1.ItemCode IS NOT NULL 
    AND PCH1.ItemCode NOT LIKE 'ZS%%'
    AND OPCH.DocDate BETWEEN {{start_date}} AND {{end_date}}
	AND PCH1.DocDate BETWEEN {{start_date}} AND {{end_date}};