SELECT
    DATEFROMPARTS(YEAR(RefDate), MONTH(RefDate), 1) AS 'agg_date',
    '11020' AS 'account_code',
    'dbs' AS 'account_name',
    'current' AS 'account_type',
    'sgd' AS 'currency',
    SUM(Debit) - SUM(Credit) AS 'amount'
FROM
    JDT1
WHERE
    Account = '11020'
GROUP BY
    DATEFROMPARTS(YEAR(RefDate), MONTH(RefDate), 1)
UNION
SELECT
    DATEFROMPARTS(YEAR(RefDate), MONTH(RefDate), 1) AS 'agg_date',
    '11040' AS 'account_code',
    'ocbc' AS 'account_name',
    'current' AS 'account_type',
    'sgd' AS 'currency',
    SUM(Debit) - SUM(Credit) AS 'amount'
FROM
    JDT1
WHERE
    Account = '11040'
GROUP BY
    DATEFROMPARTS(YEAR(RefDate), MONTH(RefDate), 1)
UNION
SELECT
    DATEFROMPARTS(YEAR(RefDate), MONTH(RefDate), 1) AS 'agg_date',
    '11043' AS 'account_code',
    'ocbc_salary' AS 'account_name',
    'current' AS 'account_type',
    'sgd' AS 'currency',
    SUM(Debit) - SUM(Credit) AS 'amount'
FROM
    JDT1
WHERE
    Account = '11043'
GROUP BY
    DATEFROMPARTS(YEAR(RefDate), MONTH(RefDate), 1)
UNION
SELECT
    DATEFROMPARTS(YEAR(RefDate), MONTH(RefDate), 1) AS 'agg_date',
    '11045' AS 'account_code',
    'ocbc_usd' AS 'account_name',
    'current' AS 'account_type',
    'usd' AS 'currency',
    SUM(Debit) - SUM(Credit) AS 'amount'
FROM
    JDT1
WHERE
    Account = '11045'
GROUP BY
    DATEFROMPARTS(YEAR(RefDate), MONTH(RefDate), 1)
UNION
SELECT
    DATEFROMPARTS(YEAR(RefDate), MONTH(RefDate), 1) AS 'agg_date',
    '11060' AS 'account_code',
    'uob' AS 'account_name',
    'current' AS 'account_type',
    'sgd' AS 'currency',
    SUM(Debit) - SUM(Credit) AS 'amount'
FROM
    JDT1
WHERE
    Account = '11060'
GROUP BY
    DATEFROMPARTS(YEAR(RefDate), MONTH(RefDate), 1)
UNION
SELECT
    DATEFROMPARTS(YEAR(RefDate), MONTH(RefDate), 1) AS 'agg_date',
    '11210' AS 'account_code',
    'dbs_fd' AS 'account_name',
    'fixed_deposit' AS 'account_type',
    'sgd' AS 'currency',
    SUM(Debit) - SUM(Credit) AS 'amount'
FROM
    JDT1
WHERE
    Account = '11210'
GROUP BY
    DATEFROMPARTS(YEAR(RefDate), MONTH(RefDate), 1)
UNION
SELECT
    DATEFROMPARTS(YEAR(RefDate), MONTH(RefDate), 1) AS 'agg_date',
    '11230' AS 'account_code',
    'ocbc_fd' AS 'account_name',
    'fixed_deposit' AS 'account_type',
    'sgd' AS 'currency',
    SUM(Debit) - SUM(Credit) AS 'amount'
FROM
    JDT1
WHERE
    Account = '11230'
GROUP BY
    DATEFROMPARTS(YEAR(RefDate), MONTH(RefDate), 1)
UNION
SELECT
    DATEFROMPARTS(YEAR(RefDate), MONTH(RefDate), 1) AS 'agg_date',
    '11260' AS 'account_code',
    'uob_fd' AS 'account_name',
    'fixed_deposit' AS 'account_type',
    'sgd' AS 'currency',
    SUM(Debit) - SUM(Credit) AS 'amount'
FROM
    JDT1
WHERE
    Account = '11260'
GROUP BY
    DATEFROMPARTS(YEAR(RefDate), MONTH(RefDate), 1)
UNION
SELECT
    DATEFROMPARTS(YEAR(RefDate), MONTH(RefDate), 1) AS 'agg_date',
    '21120' AS 'account_code',
    'dbs_tr' AS 'account_name',
    'trust_receipts' AS 'account_type',
    'sgd' AS 'currency',
    SUM(Debit) - SUM(Credit) AS 'amount'
FROM
    JDT1
WHERE
    Account = '21120'
GROUP BY
    DATEFROMPARTS(YEAR(RefDate), MONTH(RefDate), 1)
UNION
SELECT
    DATEFROMPARTS(YEAR(RefDate), MONTH(RefDate), 1) AS 'agg_date',
    '21121' AS 'account_code',
    'ocbc_tr' AS 'account_name',
    'trust_receipts' AS 'account_type',
    'sgd' AS 'currency',
    SUM(Debit) - SUM(Credit) AS 'amount'
FROM
    JDT1
WHERE
    Account = '21121'
GROUP BY
    DATEFROMPARTS(YEAR(RefDate), MONTH(RefDate), 1)
UNION
SELECT
    DATEFROMPARTS(YEAR(RefDate), MONTH(RefDate), 1) AS 'agg_date',
    '21127' AS 'account_code',
    'uob_tr' AS 'account_name',
    'trust_receipts' AS 'account_type',
    'sgd' AS 'currency',
    SUM(Debit) - SUM(Credit) AS 'amount'
FROM
    JDT1
WHERE
    Account = '21127'
GROUP BY
    DATEFROMPARTS(YEAR(RefDate), MONTH(RefDate), 1);