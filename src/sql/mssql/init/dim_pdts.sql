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
        OITB.ItmsGrpNam AS 'pdt_main_category'
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
)
SELECT
    products.*,
    pdt_avg_price.avg_price AS 'warehouse_calculated_avg_price'
FROM
    products
    LEFT JOIN pdt_avg_price ON products.pdt_code = pdt_avg_price.pdt_code