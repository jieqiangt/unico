WITH purchases AS (
    SELECT
        DocDate,
		DocStatus,
		OPCH.CardCode AS 'CardCode',
        OCRD.CardName,
        OOND.IndDesc,
        OOND.IndName,
        OCRG.GroupName,
        OCTG.PymntGroup,
        DocTotal
    FROM
        OPCH
        LEFT JOIN OCRD ON OCRD.CardCode = OPCH.CardCode
        LEFT JOIN OOND ON OOND.IndCode = OCRD.IndustryC
        LEFT JOIN OCRG ON OCRG.GroupCode = OCRD.GroupCode
        LEFT JOIN OCTG ON OCTG.GroupNum = OCRD.GroupNum
),
active_suppliers AS (
    SELECT
        DISTINCT CardCode,
        1 AS 'active_vendor'
    FROM
        OPCH
    WHERE
        DocDate >= '2021-01-01'
)
SELECT
    CONVERT(char, DocDate, 112) AS 'doc_date',
	DocStatus AS 'status',
	purchases.CardCode AS 'supplier_code',
	CardName AS 'supplier',
	IndDesc AS 'industry_desc',
	IndName AS 'inudstry_name',
    GroupName AS 'trade_suppliers_ind',
	PymntGroup AS 'payment_terms',
	COALESCE(active_vendor, 0) AS 'active_vendor',
	DocTotal AS 'total'
FROM
    purchases
	LEFT JOIN active_suppliers ON active_suppliers.CardCode = purchases.CardCode