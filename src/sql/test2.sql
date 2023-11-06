SELECT
    ORCT.DocDate AS 'Receipt Date',
    OINV.CardCode,
    OINV.CardName,
    ORCT.DocNum AS 'Receipt No.',
    COALESCE(ORCT.DocTotal,OINV.DocTotal) AS 'sumapplied',
    COALESCE(ORCT.DocTotal,OINV.DocTotal) AS 'Amount',
    'Invoice' AS 'Type',
    OINV.DocType,
    OINV.DocEntry AS 'Inv Int #',
    OINV.DocNum AS 'Inv. Num',
    OINV.DocDate AS 'Inv. Date',
    OINV.DocTotal AS 'Inv. Amt',
    OINV.GrosProfit AS 'Inv. GP',
    OSLP.SlpName AS 'Salesperson',
    OCTG.PymntGroup AS 'Payment Term',
    ORCT.Canceled
FROM
    OINV
    LEFT JOIN ORCT ON OINV.ReceiptNum = ORCT.DocEntry
    LEFT JOIN OSLP ON OINV.SlpCode = OSLP.SlpCode
    LEFT JOIN OCTG ON OINV.GroupNum = OCTG.GroupNum
WHERE
    OINV.DocDate >= [%0]
	AND OINV.DocDate <= [%1]
    AND OINV.Canceled = 'N'
    AND OINV.DocStatus = 'C'
UNION
ALL
SELECT
    ORCT.DocDate AS 'Receipt Date',
    ORIN.CardCode,
    ORIN.CardName,
    ORCT.DocNum AS 'Receipt No.',
    COALESCE(ORCT.DocTotal,ORIN.DocTotal) AS 'sumapplied',
    COALESCE(ORCT.DocTotal,ORIN.DocTotal) AS 'Amount',
    'CN' AS 'Type',
    ORIN.DocType,
    ORIN.DocEntry AS 'Inv Int #',
    ORIN.DocNum AS 'Inv. Num',
    ORIN.DocDate AS 'Inv. Date',
    ORIN.DocTotal AS 'Inv. Amt',
    ORIN.GrosProfit AS 'Inv. GP',
    OSLP.SlpName AS 'Salesperson',
    OCTG.PymntGroup AS 'Payment Term',
    ORCT.Canceled
FROM
    ORIN
    LEFT JOIN ORCT ON ORIN.ReceiptNum = ORCT.DocEntry
    LEFT JOIN OSLP ON ORIN.SlpCode = OSLP.SlpCode
    LEFT JOIN OCTG ON ORIN.GroupNum = OCTG.GroupNum
WHERE
    ORIN.DocDate >= [%0]
	AND ORIN.DocDate <= [%1]
    AND ORIN.Canceled = 'N'
    AND ORIN.DocStatus = 'C';