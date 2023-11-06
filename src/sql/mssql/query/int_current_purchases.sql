SELECT
    TRIM(CONVERT(char, PCH1.DocDate, 112)) AS 'doc_date',
    DocNum AS 'doc_num',
	PCH1.LineNum AS 'line_num',
    OPCH.CardCode AS 'supplier_code',
    OCRD.CardName AS 'supplier_name',
    PCH1.ItemCode AS 'pdt_code',
	OITM.ItemName AS 'pdt_name',
    COALESCE (PCH1.unitMsr, PCH1.unitMsr2) AS 'uom',
    PCH1.Quantity AS 'qty',
    PCH1.Price AS 'price',
    PCH1.LineTotal AS 'amount'
FROM
    PCH1
    LEFT JOIN OPCH ON PCH1.DocEntry = OPCH.DocEntry
	LEFT JOIN OITM ON PCH1.ItemCode = OITM.ItemCode
	LEFT JOIN OCRD ON OPCH.CardCode = OCRD.CardCode
    WHERE OPCH.CANCELED = 'N'
    AND DocType = 'I'
	AND OPCH.DocDate BETWEEN {{start_date}} AND {{end_date}}
	AND PCH1.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND PCH1.ItemCode IS NOT NULL 
    AND PCH1.ItemCode NOT LIKE 'ZS%%';