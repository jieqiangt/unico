WITH ar_payments AS (
    SELECT
        DATEFROMPARTS(YEAR(OINV.DocDate), MONTH(OINV.DocDate), 1) AS 'agg_date',
        SUM(OINV.DocTotal) AS 'ar_total',
        SUM(OINV.PaidToDate) AS 'ar_paid',
        SUM(OINV.DocTotal - OINV.PaidToDate) AS 'ar_outstanding'
    FROM
        OINV
        LEFT JOIN OCRD ON OINV.CardCode = OCRD.CardCode
    WHERE
        DocDate BETWEEN {{start_date}} AND {{end_date}}
        AND OINV.CANCELED = 'N'
    GROUP BY
        DATEFROMPARTS(YEAR(OINV.DocDate), MONTH(OINV.DocDate), 1)
),
ap_payments AS (
    SELECT
        DATEFROMPARTS(YEAR(DocDate), MONTH(DocDate), 1) AS 'agg_date',
        SUM(OPCH.DocTotal) AS 'ap_total',
        SUM(OPCH.PaidToDate) AS 'ap_paid',
        SUM(OPCH.DocTotal - OPCH.PaidToDate) AS 'ap_outstanding'
    FROM
        OPCH
        LEFT JOIN OCRD ON OPCH.CardCode = OCRD.CardCode
    WHERE
        DocDate BETWEEN {{start_date}} AND {{end_date}}
        AND CANCELED = 'N'
    GROUP BY
        DATEFROMPARTS(YEAR(OPCH.DocDate), MONTH(OPCH.DocDate), 1)
)
SELECT
    ar_payments.agg_date,
    ar_total,
    ar_paid,
    ar_outstanding,
    ap_total,
    ap_paid,
    ap_outstanding,
    ar_total - ap_total AS 'total_diff',
    ar_outstanding - ap_outstanding AS 'outstanding_diff'
FROM
    ar_payments
    LEFT JOIN ap_payments ON ar_payments.agg_date = ap_payments.agg_date;