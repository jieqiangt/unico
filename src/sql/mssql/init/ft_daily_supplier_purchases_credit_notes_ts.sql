WITH purchases AS (
    SELECT
        DATEFROMPARTS(
            YEAR(PCH1.DocDate),
            MONTH(PCH1.DocDate),
            1
        ) AS 'start_of_month_date',
        PCH1.DocDate AS 'as_of_date',
        OPCH.CardCode AS 'supplier_code',
        PCH1.ItemCode AS 'pdt_code',
        AVG(PCH1.Price) AS 'purchase_price',
        SUM(PCH1.Quantity) AS 'purchase_qty',
        SUM(PCH1.LineTotal) AS 'purchase_value'
    FROM
        PCH1
        LEFT JOIN OPCH ON PCH1.DocEntry = OPCH.DocEntry
        LEFT JOIN OCRD ON OPCH.CardCode = OCRD.CardCode
    WHERE
        OPCH.Canceled = 'N'
        AND PCH1.DocDate BETWEEN {{start_date}} AND {{end_date}}
        AND PCH1.Price > 0.01
        AND (
            (CHARINDEX('ZS', PCH1.ItemCode) = 0)
            OR (PCH1.ItemCode IS NULL)
        )
    GROUP BY
        PCH1.DocDate,
        OPCH.CardCode,
        PCH1.ItemCode
),
credit_notes AS (
    SELECT
        DATEFROMPARTS(
            YEAR(RPC1.DocDate),
            MONTH(RPC1.DocDate),
            1
        ) AS 'start_of_month_date',
        RPC1.DocDate AS 'as_of_date',
        ORPC.CardCode AS 'supplier_code',
        RPC1.ItemCode As 'pdt_code',
        SUM(RPC1.LineTotal) AS 'credit_note_value'
    FROM
        RPC1
        LEFT JOIN ORPC ON RPC1.DocEntry = ORPC.DocEntry
    WHERE
        RPC1.DocDate BETWEEN {{start_date}} AND {{end_date}}
        AND ORPC.Canceled = 'N'
        AND RPC1.Price > 0.01
        AND CHARINDEX('ZS', RPC1.ItemCode) = 0
    GROUP BY
        RPC1.DocDate,
        ORPC.CardCode,
        RPC1.ItemCode
)
SELECT
    purchases.start_of_month_date,
	purchases.as_of_date,
	COALESCE(purchases.supplier_code, credit_notes.supplier_code) AS 'supplier_code',
	COALESCE(purchases.pdt_code, credit_notes.pdt_code) AS 'pdt_code',
	COALESCE(purchases.purchase_price, 0) AS 'purchase_price',
	COALESCE(purchases.purchase_qty,0) AS 'purchase_qty',
	COALESCE(purchases.purchase_value,0) AS 'purchase_value',
	COALESCE(credit_notes.credit_note_value,0) AS 'credit_note_value',
    COALESCE(purchases.purchase_value,0) + COALESCE(credit_notes.credit_note_value,0) AS 'total_value'
FROM
    purchases
    LEFT JOIN credit_notes ON purchases.start_of_month_date = credit_notes.start_of_month_date
    AND purchases.as_of_date = credit_notes.as_of_date
    AND purchases.supplier_code = credit_notes.supplier_code
    AND purchases.pdt_code = credit_notes.pdt_code