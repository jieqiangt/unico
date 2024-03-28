WITH products AS (
    SELECT
        OITM.ItemCode AS 'pdt_code',
        OITM.ItemName AS 'pdt_name',
        OITM.FrgnName AS 'foreign_pdt_name',
        OITM.InvntryUoM as 'uom',
        CASE
            WHEN OITM.QryGroup2 = 'Y' THEN 1
            ELSE 0
        END AS 'processed_pdt_ind',
        OITM.ValidFor AS 'is_active',
        CASE
            WHEN DATEDIFF(week, OITM.CreateDate, GETDATE()) < 3 THEN 'new'
            ELSE 'old'
        END AS 'new_pdt_ind',
        OITM.LstEvlPric AS 'base_price',
        OITB.ItmsGrpNam AS 'pdt_main_category',
        OITM.LastPurDat AS 'last_purchase_date',
        OITM.LastPurPrc AS 'last_purchase_price'
    FROM
        OITM
        LEFT JOIN OITB ON OITM.ItmsGrpCod = OITB.ItmsGrpCod
),
pdt_avg_price AS (
    SELECT
        OINM.ItemCode AS 'pdt_code',
        SUM(OINM.TransValue) / SUM(OINM.InQty - OINM.OutQty) AS 'avg_price'
    FROM
        OINM
        INNER JOIN OITM ON OINM.ItemCode = OITM.ItemCode
    WHERE
        OINM.DocDate <= (
            SELECT
                DATEFROMPARTS(
                    YEAR(GetDate()),
                    MONTH(GetDate()),
                    DAY(GetDate())
                )
        )
    GROUP BY
        OINM.ItemCode
    HAVING
        SUM(OINM.InQty - OINM.OutQty) <> 0
),
ecommerce_pdts AS (
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
	AND OOND.IndName = 'E-COMMERCE'
)
SELECT
    products.*,
    pdt_avg_price.avg_price AS 'warehouse_calculated_avg_price',
    CASE WHEN ecommerce_pdts.pdt_code IS NULL THEN 'N' ELSE 'Y' END AS 'ecommerce_pdt_ind'
FROM
    products
    LEFT JOIN pdt_avg_price ON products.pdt_code = pdt_avg_price.pdt_code
    LEFT JOIN ecommerce_pdts ON products.pdt_code = ecommerce_pdts.pdt_code;

