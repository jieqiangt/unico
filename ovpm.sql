SELECT
    OVPM.DocEntry AS 'doc_entry',
    DocNum AS 'doc_num',
    CONVERT(char, DocDate, 112) AS 'doc_date',
    DocType AS 'doc_type',
    DocCurr AS 'currency',
    CashSum AS 'cash_sum',
    CashSumFC AS 'cash_sum_fc',
    CreditSum AS 'credit_sum',
    CredSumFC AS 'credit_sum_fc',
    CheckSum AS 'check_sum',
    CheckSumFC AS 'check_sum_fc',
    TrsfrSum AS 'transfer_sum',
    TrsfrSumFc AS 'transfer_sum_fc',
    DocTotal AS 'doc_total',
    DocTotalFC AS 'doc_total_fc',
    VatSum AS 'vat_sum',
    VatSumFC AS 'vat_sum_fc',
    CONVERT(char, TrsfrDate, 112) AS 'transfer_date',
    Ref1 AS 'ref_1',
    Ref2 AS 'ref_2',
    CounterRef AS 'counter_ref',
    Comments AS 'comments',
    JrnlMemo AS 'memo'
FROM
    OVPM
    LEFT JOIN OCRD ON OVPM.CardCode = OCRD.CardCode
WHERE
    DocDate BETWEEN '2014-01-01'
    AND '2023-08-14'
    AND Canceled = 'N';