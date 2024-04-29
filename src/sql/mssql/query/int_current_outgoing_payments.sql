SELECT
    DATEFROMPARTS(YEAR(OVPM.DocDate), MONTH(OVPM.DocDate), 1) AS 'agg_date',
    DocDate AS 'doc_date',
    DocNum AS 'doc_num',
    OVPM.CardCode AS 'supplier_code',
    OCRD.CardName AS 'supplier_name',
    DocTotal AS 'amount'
FROM
    OVPM
    LEFT JOIN OCRD ON OVPM.CardCode = OCRD.CardCode
WHERE
    OVPM.Canceled = 'N'
    AND DocDate BETWEEN {{start_date}} AND {{end_date}};