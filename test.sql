SELECT
    P.SlpName,
    [1] as [Jan],
    [2] as [Feb],
    [3] as [Mar],
    [4] as [Apr],
    [5] as [May],
    [6] as [Jun],
    [7] as [Jul],
    [8] as [Aug],
    [9] as [Sep],
    [10] as [Oct],
    [11] as [Nov],
    [12] as [Dec]
FROM
    (
        SELECT
            OSLP.SlpName,
            MONTH(OINV.DocDate) as 'sales_month',
			OINV.DocTotal - OINV.VatSum AS 'total'
        FROM
            OINV
            LEFT JOIN OCRD ON OINV.CardCode = OCRD.CardCode
            LEFT JOIN OSLP ON OCRD.SlpCode = OSLP.SlpCode
        WHERE
            OINV.DocDate BETWEEN [%0] AND [%01]
			AND OINV.Canceled = 'N'
			AND OINV.DocStatus = 'C'
        UNION ALL
        SELECT
            OSLP.SlpName,
            MONTH(ORIN.docdate) AS 'sales_month',
			ORIN.DocTotal - ORIN.VatSum AS 'total'
        FROM
            ORIN
            LEFT JOIN OCRD ON ORIN.CardCode = OCRD.CardCode
            LEFT JOIN OSLP ON OCRD.SlpCode = OSLP.SlpCode
        WHERE
            ORIN.DocDate BETWEEN [%0] AND [%01]
			AND ORIN.Canceled = 'N'
			AND ORIN.DocStatus = 'C'
    ) S PIVOT (
        SUM(total) FOR sales_month IN (
            [1],
            [2],
            [3],
            [4],
            [5],
            [6],
            [7],
            [8],
            [9],
            [10],
            [11],
            [12]
        )
    ) P
ORDER BY
    P.[SALES EMPLOYEE];

WITH invoice AS (
    SELECT
        'join_key' AS 'join_key',
        SUM(OINV.DocTotal) AS 'invoice_amt'
    FROM
        OINV
        LEFT JOIN OCRD ON OINV.CardCode = OCRD.CardCode
        LEFT JOIN OSLP ON OCRD.SlpCode = OSLP.SlpCode
    WHERE
        MONTH(OINV.DocDate) = 1
        AND YEAR(OINV.DocDate) = 2024
        AND OINV.CANCELED = 'N'
        AND DocStatus = 'C'
),
credit_note AS (
    SELECT
        'join_key' AS 'join_key',
        SUM(ORIN.DocTotal) AS 'cn_amt'
    FROM
        ORIN
        LEFT JOIN OCRD ON ORIN.CardCode = OCRD.CardCode
        LEFT JOIN OSLP ON OCRD.SlpCode = OSLP.SlpCode
    WHERE
        MONTH(ORIN.DocDate) = 1
        AND YEAR(ORIN.DocDate) = 2024
        AND ORIN.CANCELED = 'N'
        AND DocStatus = 'C'
)
SELECT
    invoice_amt,
    cn_amt,
    (invoice_amt + cn_amt) AS 'total'
FROM
    invoice
    LEFT JOIN credit_note ON invoice.join_key = credit_note.join_key