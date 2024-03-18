SELECT
	TRIM(CONVERT(char, DocDate, 112)) AS 'doc_date',
	OPCH.CardCode AS 'supplier_code',
	DocTotal AS 'doc_total'
FROM
	OPCH
WHERE
	CANCELED = 'N';