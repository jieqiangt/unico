WITH sales AS (
    SELECT
    ORDR.DocNum AS 'sales_order_doc_num',
    ORDR.DocDate AS 'sales_order_date',
    ORDR.DocStatus AS 'sales_order_status',
    DATEDIFF(day, ORDR.DocDate, ORCT.DocDate) AS 'sales_order_days_to_receipt',
    ORDR.GrosProfit AS 'sales_order_gross_profit',
    ROUND(ORDR.GrosProfit / NULLIF(ORDR.DocTotal, 0), 3) AS 'sales_order_gp_margin',
    OINV.DocNum AS 'ar_invoice_doc_num',
    OINV.DocDate AS 'ar_invoice_date',
    OINV.DocStatus AS 'ar_status',
    DATEDIFF(day, OINV.DocDate, ORCT.DocDate) AS 'ar_days_to_receipt',
    OINV.GrosProfit AS 'ar_gross_profit',
    ROUND(OINV.GrosProfit / NULLIF(OINV.DocTotal, 0), 3) AS 'ar_gp_margin',
    OCTG.PymntGroup AS 'payment_terms',
    OCRD.CardCode AS 'customer_code',
    OCRD.CardName AS 'customer_name',
    OSLP.SlpName AS 'sales_employee',
    ORCT.DocDate AS 'incoming_payment_date',
    ORCT.DocNum AS 'incoming_payment_doc_num'
FROM
    ORDR
    LEFT JOIN OSLP ON ORDR.SlpCode = OSLP.SlpCode
    LEFT JOIN OCRD ON ORDR.CardCode = OCRD.CardCode
    LEFT JOIN OCTG ON OCRD.GroupNum = OCTG.GroupNum
    LEFT JOIN OINV ON ORDR.DocNum = OINV.Ref1
    LEFT JOIN RCT2 ON OINV.DocEntry = RCT2.DocEntry
    LEFT JOIN ORCT ON RCT2.DocNum = ORCT.DocEntry
WHERE
    ORDR.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND ORDR.CANCELED = 'N'
    AND OINV.CANCELED = 'N'
    AND ORCT.Canceled = 'N'
)
SELECT
    sales_order_doc_num,
    sales_order_date,
    sales_order_status,
    MAX(sales_order_days_to_receipt) AS 'sales_order_days_to_receipt',
    AVG(sales_order_gross_profit) AS 'sales_order_gross_profit',
    AVG(sales_order_gp_margin) AS 'sales_order_gp_margin',
    ar_invoice_doc_num,
    ar_invoice_date,
    ar_status,
    MAX(ar_days_to_receipt) AS 'ar_days_to_receipt',
    AVG(ar_gross_profit) AS 'ar_gross_profit',
    AVG(ar_gp_margin) AS 'ar_gp_margin',
    payment_terms,
    sales_employee,
    customer_code,
    customer_name,
    incoming_payment_date,
    incoming_payment_doc_num
FROM 
sales 
GROUP BY sales_order_doc_num, sales_order_date, sales_order_status, ar_invoice_doc_num, ar_invoice_date, ar_status, payment_terms, sales_employee, incoming_payment_date, incoming_payment_doc_num, customer_code, customer_name