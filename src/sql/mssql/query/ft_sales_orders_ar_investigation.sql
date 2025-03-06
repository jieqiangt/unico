SELECT
    'Sales Order' AS 'originating_doc_type',
    ORDR.DocNum AS 'originating_doc_num',
    ORDR.DocDate AS 'originating_doc_date',
    ORDR.DocStatus AS 'originating_doc_status',
    OINV.DocNum AS 'ar_invoice_doc_num',
    OINV.DocDate AS 'ar_invoice_date',
    OINV.DocStatus AS 'ar_status',
    OCTG.PymntGroup AS 'payment_terms',
    OCRD.CardCode AS 'customer_code',
    OCRD.CardName AS 'customer_name',
    OSLP.SlpName AS 'sales_employee',
    ORDR.DocTotal AS 'originating_doc_total',
    OINV.DocTotal AS 'ar_doc_total',
    ORDR.DocTotal - OINV.DocTotal AS 'doc_total_diff',
    (ORDR.DocTotal - OINV.DocTotal)/NULLIF(ORDR.DocTotal,0) AS 'doc_total_percentage_diff',
    ORDR.GrosProfit AS 'originating_doc_gross_profit',
    OINV.GrosProfit AS 'ar_gross_profit',
    ORDR.GrosProfit - OINV.GrosProfit AS 'gross_profit_diff',
    (ORDR.GrosProfit - OINV.GrosProfit)/NULLIF(ORDR.GrosProfit,0) AS 'gross_profit_percentage_diff'
FROM
    ORDR
    LEFT JOIN OSLP ON ORDR.SlpCode = OSLP.SlpCode
    LEFT JOIN OCRD ON ORDR.CardCode = OCRD.CardCode
    LEFT JOIN OCTG ON OCRD.GroupNum = OCTG.GroupNum
    LEFT JOIN OINV ON ORDR.DocNum = OINV.Ref1
WHERE
    ORDR.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND ORDR.CANCELED = 'N'
    AND OINV.CANCELED = 'N'
