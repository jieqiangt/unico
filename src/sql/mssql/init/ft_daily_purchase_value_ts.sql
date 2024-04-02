SELECT
    DATEFROMPARTS(YEAR(OPCH.DocDate), MONTH(OPCH.DocDate), DAY(OPCH.DocDate)) AS 'as_of_date',
    COALESCE(OOND.IndDesc, 'LOCAL SUPPLIERS') AS 'local_overseas_ind',
    SUM(OPCH.DocTotal) AS 'amount_with_tax'
FROM
    OPCH
LEFT JOIN OCRD ON OPCH.CardCode = OCRD.CardCode
LEFT JOIN OOND ON OCRD.IndustryC = OOND.IndCode
WHERE
    OPCH.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND OPCH.CANCELED = 'N'
GROUP BY
    OPCH.DocDate, OOND.IndDesc
ORDER BY
    OPCH.DocDate, OOND.IndDesc