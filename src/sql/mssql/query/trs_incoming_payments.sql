SELECT
    DATEFROMPARTS(YEAR(ORCT.DocDate), MONTH(ORCT.DocDate), 1) AS 'start_of_month',
    DocDate AS 'doc_date',
    DocNum AS 'doc_num',
    ORCT.CardCode AS 'customer_code',
    DocTotal AS 'amount'
FROM
    ORCT
WHERE
    ORCT.Canceled != 'N'
    AND ORCT.DocDate BETWEEN {{start_date}} AND {{end_date}};