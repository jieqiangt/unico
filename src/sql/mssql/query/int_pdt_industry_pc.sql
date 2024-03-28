WITH filtered_sales AS (
	SELECT
		OITM.ItemCode AS 'pdt_code',
		COALESCE(OOND.IndName, 'HoReCa') AS 'industry',
		price,
		OITM.LastPurPrc AS 'last_purchase_price'
	FROM
		RDR1
		LEFT JOIN OITM ON RDR1.ItemCode = OITM.ItemCode
		LEFT JOIN ORDR ON RDR1.DocEntry = ORDR.DocEntry
		LEFT JOIN OCRD ON ORDR.CardCode = OCRD.CardCode
		LEFT JOIN OOND ON OCRD.IndustryC = OOND.IndCode
		LEFT JOIN OITB ON OITM.ItmsGrpCod = OITB.ItmsGrpCod
	WHERE
		RDR1.DocDate BETWEEN {{start_date}} AND {{end_date}}
		AND ORDR.DocDate BETWEEN {{start_date}} AND {{end_date}}
		AND RDR1.ItemCode IS NOT NULL
		AND RDR1.price > 0.01
		AND ORDR.Canceled = 'N'
		AND (
			OOND.IndName IS NULL
			OR OOND.IndName NOT IN (
				'E-COMMERCE',
				'CASH RELATED',
				'BAD DEBT',
				'LOCAL SUP',
				'OVERSEAS SUP'
			)
		)
),
agg_metrics AS (
	SELECT
		pdt_code,
		industry,
		MIN(price) AS 'min_price',
		MAX(price) AS 'max_price',
		MAX(last_purchase_price) AS 'last_purchase_price'
	FROM
		filtered_sales
	GROUP BY
		pdt_code,
		industry
)
SELECT
	pdt_code,
	industry,
	min_price,
	(min_price - last_purchase_price) /(min_price) AS 'per_last_purchase_price_min_pc1',
	max_price,
	(max_price - last_purchase_price) /(max_price) AS 'per_last_purchase_price_max_pc1'
FROM
	agg_metrics;