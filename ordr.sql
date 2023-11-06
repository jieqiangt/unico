SELECT
    CONVERT(char, ORDR.CreateDate, 112) AS 'create_date',
    CONVERT(char, ORDR.UpdateDate, 112) AS 'update_date',
    CONVERT(char, ORDR.DocDate, 112) AS 'doc_date',
    DocTime AS "doc_time",
    CONVERT(char, ORDR.DocDueDate, 112) AS 'doc_due_date',
    ORDR.DocEntry AS 'id',
    DocNum AS 'doc_no',
    (
        CASE
            WHEN DocType = 'I' THEN 'item'
            ELSE 'service'
        END
    ) AS 'doc_type',
    (
        CASE
            WHEN Canceled = 'N' THEN 0
            ELSE 1
        END
    ) AS 'cancelled_ind',
    (
        CASE
            WHEN Printed = 'N' THEN 0
            ELSE 1
        END
    ) AS 'printed_ind',
    (
        CASE
            WHEN DocStatus = 'C' THEN 'closed'
            ELSE 'open'
        END
    ) AS 'doc_status',
    VatSum AS 'vat_total',
    DocTotal AS 'total',
    PaidSum AS 'paid_total',
    ORDR.CardCode AS 'customer_code',
    OCRD.CardName AS 'customer_name',
    OCRD.Address AS 'customer_address',
    OCRD.ZipCode AS 'customer_zip_code',
    OCTG.PymntGroup AS 'terms_of_payment',
    OCTG.ExtraDays AS 'extra_payment_days',
    JrnlMemo AS 'memo_desc',
    OSLP.SlpName AS 'sales_agent',
    (
        CASE
            WHEN IsCrin = 'N' THEN 0
            ELSE 1
        END
    ) AS 'corrected_ind',
    ShipToCode AS 'customer_to_ship_to',
    ORDR.OwnerCode AS 'doc_owner_emp_id',
    OHEM.lastName AS 'doc_owner_name',
    PaytoCode AS 'customer_to_bill_to',
    (
        CASE
            WHEN isIns = 'N' THEN 0
            ELSE 1
        END
    ) AS 'reverse_invoice_ind',
    TrackNo AS 'tracking_no',
    (
        CASE
            WHEN RetInvoice = 'N' THEN 0
            ELSE 1
        END
    ) AS 'return_ind',
    CONVERT(char, ORDR.ReqDate, 112) AS 'required_date',
    CONVERT(char, ORDR.CancelDate, 112) AS 'cancel_date',
    CONVERT(char, ORDR.ClsDate, 112) AS 'closing_date'
FROM
    ORDR
    LEFT JOIN OCRD ON ORDR.CardCode = OCRD.CardCode
    LEFT JOIN OCTG ON ORDR.GroupNum = OCTG.GroupNum
    LEFT JOIN OSLP ON ORDR.SlpCode = OSLP.SlpCode
    LEFT JOIN OHEM ON ORDR.OwnerCode = OHEM.empID