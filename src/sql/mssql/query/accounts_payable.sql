SELECT
    TRIM(CONVERT(char, DocDate, 112)) AS 'doc_date',
    TRIM(CONVERT(char, DocDueDate, 112)) AS 'due_date',
    DocNum AS 'doc_num',
    OPCH.CardCode AS 'bp_code',
    OCRD.CardName AS 'bp_name',
    OCRD.CardType AS 'bp_type',
    OCTG.PymntGroup AS 'payment_terms',
    OPCH.ExtraDays AS 'extra_payment_days',
    Memo AS 'journal_memo',
    DocTotal AS 'total_payable',
    VatSum AS 'tax_amount',
    SysTotal AS 'transaction_total',
    (DocTotal - SysTotal) AS 'payable_amt_left',
    CASE
        WHEN (DocTotal - SysTotal) > 0 THEN 'OPEN'
        ELSE 'CLOSED'
    END AS 'status'
FROM
    OPCH
    LEFT JOIN OCRD ON OPCH.CardCode = OCRD.CardCode
    LEFT JOIN OCTG ON OPCH.GroupNum = OCTG.GroupNum
    LEFT JOIN OJDT ON OPCH.TransId = OJDT.TransId
WHERE
    CANCELED != 'Y'
    AND Confirmed = 'Y'
    AND SysTotal > 0
    AND DocDate > '2023-09-04';