SELECT
    'Credit Note' AS 'originating_doc_type',
    ORIN.DocNum AS 'originating_doc_num',
    ORIN.DocDate AS 'originating_doc_date',
    ORIN.DocStatus AS 'originating_doc_status',
    OINV.DocNum AS 'ar_invoice_doc_num',
    OINV.DocDate AS 'ar_invoice_date',
    OINV.DocStatus AS 'ar_status',
    OCTG.PymntGroup AS 'payment_terms',
    OCRD.CardCode AS 'customer_code',
    OCRD.CardName AS 'customer_name',
    OSLP.SlpName AS 'sales_employee',
    ORIN.DocTotal AS 'originating_doc_total',
    OINV.DocTotal AS 'ar_doc_total',
    ORIN.DocTotal - OINV.DocTotal AS 'doc_total_diff',
    (ORIN.DocTotal - OINV.DocTotal)/NULLIF(ORIN.DocTotal, 0) AS 'doc_total_percentage_diff',
    ORIN.GrosProfit AS 'originating_doc_gross_profit',
    OINV.GrosProfit AS 'ar_gross_profit',
    ORIN.GrosProfit - OINV.GrosProfit AS 'gross_profit_diff',
    (ORIN.GrosProfit - OINV.GrosProfit)/NULLIF(ORIN.GrosProfit, 0) AS 'gross_profit_percentage_diff'
FROM
    ORIN
    LEFT JOIN OSLP ON ORIN.SlpCode = OSLP.SlpCode
    LEFT JOIN OCRD ON ORIN.CardCode = OCRD.CardCode
    LEFT JOIN OCTG ON OCRD.GroupNum = OCTG.GroupNum
    INNER JOIN RIN1 ON ORIN.DocEntry = RIN1.DocEntry
    LEFT JOIN INV1 ON RIN1.BaseEntry = INV1.DocEntry
    AND RIN1.BaseLine = INV1.LineNum
    AND RIN1.BaseType = 13
    INNER JOIN OINV ON INV1.DocEntry = OINV.DocEntry
WHERE 
    ORIN.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND ORIN.CANCELED = 'N'
    AND OINV.CANCELED = 'N';