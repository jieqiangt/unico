SELECT
    DATEFROMPARTS(YEAR(OVPM.DocDate), MONTH(OVPM.DocDate), 1) AS 'start_of_month',
    DATEFROMPARTS(
        YEAR(DocDate),
        MONTH(DocDate),
        DAY(DocDate)
    ) AS 'doc_date',
    OVPM.CardCode AS 'supplier_code',
    SUM(DocTotal) AS 'amount'
FROM
    OVPM
WHERE
    OVPM.Canceled = 'N'
    AND DocDate BETWEEN {{start_date}} AND {{end_date}}
GROUP BY
    DocDate,
    CardCode;