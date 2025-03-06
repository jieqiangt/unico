SELECT
    ORDR.DocNum AS 'sales_order_doc_num',
    ORDR.DocDate AS 'sales_order_date',
    DATEDIFF(day, ORDR.DocDate, ORCT.DocDate) AS 'days_to_receipt',
    OCTG.PymntGroup AS 'payment_terms',
    OSLP.SlpName AS 'salesperson',
    ORDR.GrosProfit AS 'gross_profit',
    ROUND(ORDR.GrosProfit / NULLIF(ORDR.DocTotal, 0), 3) AS 'gp_margin'
FROM
    ORDR
    LEFT JOIN OINV ON ORDR.DocNum = OINV.Ref1
    LEFT JOIN OSLP ON OINV.SlpCode = OSLP.SlpCode
    LEFT JOIN OCRD ON OINV.CardCode = OCRD.CardCode
    LEFT JOIN OCTG ON OCRD.GroupNum = OCTG.GroupNum
    LEFT JOIN RCT2 ON OINV.DocEntry = RCT2.DocEntry
    LEFT JOIN ORCT ON RCT2.DocNum = ORCT.DocEntry
WHERE
    RCT2.InvType = 13
    AND ORDR.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND ORDR.DocStatus = 'C'
    AND OINV.DocStatus = 'C'