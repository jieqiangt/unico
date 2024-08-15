SELECT
    DATEFROMPARTS(YEAR(OVPM.DocDate), MONTH(OVPM.DocDate), 1) AS 'start_of_month',
    DocDate AS 'doc_date',
    DocNum AS 'doc_num',
    OVPM.CardCode AS 'supplier_code',
    DocTotal AS 'amount'
FROM
    OVPM
WHERE
    OVPM.Canceled = 'N'
    AND DocDate BETWEEN {{start_date}} AND {{end_date}};