WITH ap_payments AS (
    SELECT
        DATEFROMPARTS(YEAR(DocDate), MONTH(DocDate), 1) AS 'agg_date',
        GroupName AS 'trade_classification',
        SUM(PaidToDate) 'paid',
        SUM(DocTotal - PaidToDate) AS 'outstanding_amt'
    FROM
        OPCH
        LEFT JOIN OCRD ON OPCH.CardCode = OCRD.CardCode
        LEFT JOIN OCRG ON OCRD.GroupCode = OCRG.GroupCode
    WHERE
        DocDate BETWEEN {{start_date}} AND {{end_date}}
        AND CANCELED = 'N'
    GROUP BY
        DATEFROMPARTS(YEAR(DocDate), MONTH(DocDate), 1),
        OCRG.GroupName
),
ap_payments_long AS (
    SELECT
        agg_date,
        'accounts_payable' AS 'account_type',
        'C' AS 'payment_status',
        trade_classification,
        'paid' AS 'amount_type',
        paid AS 'amount'
    FROM
        ap_payments
    UNION
    ALL
    SELECT
        agg_date,
        'accounts_payable' AS 'account_type',
        'O' AS 'payment_status',
        trade_classification,
        'outstanding' AS 'amount_type',
        outstanding_amt AS 'amount'
    FROM
        ap_payments
),
ar_payments AS (
    SELECT
        DATEFROMPARTS(YEAR(OINV.DocDate), MONTH(OINV.DocDate), 1) AS 'agg_date',
        OCRG.GroupName AS 'trade_classification',
        SUM(OINV.PaidToDate) AS 'paid',
        SUM(OINV.DocTotal - OINV.PaidToDate) AS 'outstanding_amt'
    FROM
        OINV
        LEFT JOIN OCRD ON OINV.CardCode = OCRD.CardCode
        LEFT JOIN OCRG ON OCRD.GroupCode = OCRG.GroupCode
    WHERE
        DocDate BETWEEN {{start_date}} AND {{end_date}}
        AND OINV.CANCELED = 'N'
    GROUP BY
        DATEFROMPARTS(YEAR(OINV.DocDate), MONTH(OINV.DocDate), 1),
        OCRG.GroupName
),
ar_payments_long AS (
    SELECT
        agg_date,
        'accounts_receivable' AS 'account_type',
        'C' AS 'payment_status',
        trade_classification,
        'paid' AS 'amount_type',
        paid AS 'amount'
    FROM
        ar_payments
    UNION
    ALL
    SELECT
        agg_date,
        'accounts_receivable' AS 'account_type',
        'O' AS 'payment_status',
        trade_classification,
        'outstanding' AS 'amount_type',
        outstanding_amt AS 'amount'
    FROM
        ar_payments
)
SELECT
    agg_date,
    account_type,
    payment_status,
    trade_classification,
    amount_type,
    SUM(amount) AS 'amount'
FROM
    ap_payments_long
GROUP BY
    agg_date,
    account_type,
    payment_status,
    trade_classification,
    amount_type
UNION
ALL
SELECT
    agg_date,
    account_type,
    payment_status,
    trade_classification,
    amount_type,
    SUM(amount) AS 'amount'
FROM
    ar_payments_long
GROUP BY
    agg_date,
    account_type,
    payment_status,
    trade_classification,
    amount_type