SELECT
    DATEFROMPARTS(YEAR(OPCH.DocDate), MONTH(OPCH.DocDate), DAY(OPCH.DocDate)) AS 'as_of_date',
    SUM(OPCH.DocTotal) AS 'amount_with_tax'
FROM
    OPCH
WHERE
    OPCH.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND OPCH.CANCELED = 'N'
GROUP BY
    OPCH.DocDate
ORDER BY
    OPCH.DocDate