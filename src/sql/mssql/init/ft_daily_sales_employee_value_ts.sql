SELECT
    DATEFROMPARTS(YEAR(ORDR.DocDate), MONTH(ORDR.DocDate), DAY(ORDR.DocDate)) AS 'as_of_date',
    DATEFROMPARTS(YEAR(ORDR.DocDate), MONTH(ORDR.DocDate), 1) AS 'agg_date',
    ORDR.SlpCode AS 'sales_employee_code',
    OSLP.SlpName AS 'sales_employee_name',
    SUM(ORDR.DocTotal) AS 'value'
FROM
    ORDR
    LEFT JOIN OSLP ON ORDR.SlpCode = OSLP.SlpCode
WHERE
    ORDR.DocDate BETWEEN {{start_date}} AND {{end_date}}
GROUP BY
    ORDR.DocDate,
    DATEFROMPARTS(YEAR(ORDR.DocDate), MONTH(ORDR.DocDate), 1),
    ORDR.SlpCode,
    OSLP.SlpName;