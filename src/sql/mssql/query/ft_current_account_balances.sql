SELECT
    DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AS 'agg_date',
    {{as_of_date}} AS 'as_of_date',
    AcctCode AS 'account_code',
    AcctName AS 'account_name',
    CurrTotal AS 'amount'
FROM
    OACT
WHERE
    AcctCode IN (
        '11020',
        '11040',
        '11043',
        '11045',
        '11060',
        '11210',
        '11230',
        '11260'
    )
UNION
SELECT
    DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AS 'agg_date',
    {{as_of_date}} AS 'as_of_date',
    AcctCode AS 'account_code',
    AcctName AS 'account_name',
    CurrTotal * -1 AS 'amount'
FROM
    OACT
WHERE
    AcctCode IN (
        '21120',
        '21121',
        '21127',
        '20110',
        '20150',
        '20160',
        '20170',
        '21230'
    );