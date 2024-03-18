SELECT
    CONVERT(DATE, OIGN.DocDate) AS 'doc_date',
    ItemCode AS 'pdt_code',
    SUM(Quantity) AS 'qty',
    SUM(LineTotal) AS 'value'
FROM
    OIGN
    INNER JOIN IGN1 ON OIGN.DocEntry = IGN1.DocEntry
WHERE
    OIGN.DocDate BETWEEN {{start_date}}
    AND {{end_date}}
GROUP BY
    DATEFROMPARTS(YEAR(OIGN.DocDate), MONTH(OIGN.DocDate), 1),
    CONVERT(DATE, OIGN.DocDate),
    ItemCode;