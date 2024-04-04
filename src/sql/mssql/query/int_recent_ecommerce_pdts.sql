SELECT
	DISTINCT
    RDR1.ItemCode AS 'pdt_code'
FROM
    RDR1
	LEFT JOIN ORDR ON RDR1.DocEntry = ORDR.DocEntry
	LEFT JOIN OCRD ON ORDR.CardCode = OCRD.CardCode
	LEFT JOIN OOND ON OCRD.IndustryC = OOND.IndCode
WHERE
    RDR1.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND ORDR.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND ORDR.Canceled = 'N'
    AND RDR1.ItemCode IS NOT NULL
	AND RDR1.Price > 0.01
	AND OOND.IndName = 'E-COMMERCE';