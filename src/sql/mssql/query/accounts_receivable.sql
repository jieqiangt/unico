SELECT
    TRIM(CONVERT(char, ORDR.DocDate, 112)) AS 'doc_date',
    TRIM(CONVERT(char, ORDR.DocDueDate, 112)) AS 'due_date',
    ORDR.DocNum AS 'doc_num',
    ORDR.CardCode AS 'bp_code',
    OCRD.CardName AS 'bp_name',
    OCRD.CardType AS 'bp_type',
    OCTG.PymntGroup AS 'payment_terms',
    ORDR.ExtraDays AS 'extra_payment_days',
    Memo AS 'journal_memo',
    OINV.DocTotal AS 'total_receivable',
    CASE
        WHEN OINV.DocTotal IS NULL THEN ORDR.DocTotal
        ELSE (ORDR.DocTotal - OJDT.SysTotal)
    END AS 'receivable_amt_left',
    CASE
        WHEN (ORDR.DocTotal - OJDT.SysTotal) = 0 THEN 'CLOSED'
        ELSE 'OPEN'
    END AS 'status'
FROM
    ORDR
    LEFT JOIN OINV ON ORDR.DocNum = OINV.DocNum
    LEFT JOIN OCRD ON ORDR.CardCode = OCRD.CardCode
    LEFT JOIN OCTG ON ORDR.GroupNum = OCTG.GroupNum
    LEFT JOIN OJDT ON OINV.TransId = OJDT.TransId
WHERE
    ORDR.CANCELED != 'Y'
    AND ORDR.Confirmed = 'Y'
    AND ORDR.DocDate > '2023-09-04'