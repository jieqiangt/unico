WITH to_processing AS (
    SELECT
        DATEFROMPARTS(
            YEAR(IGE1.DocDate),
            MONTH(IGE1.DocDate),
            DAY(IGE1.DocDate)
        ) AS 'as_of_date',
        'Processing Movement' AS 'value_category',
        'TO PROCESSING' AS 'value_sub_category',
        SUM(LineTotal) AS 'value'
    FROM
        IGE1
    WHERE
        DocDate BETWEEN {{start_date}} AND {{end_date}}
    GROUP BY
        DocDate
),
from_processing AS (
    SELECT
        DATEFROMPARTS(
            YEAR(IGN1.DocDate),
            MONTH(IGN1.DocDate),
            DAY(IGN1.DocDate)
        ) AS 'as_of_date',
        'Processing Movement' AS 'value_category',
        'FROM PROCESSING' AS 'value_sub_category',
        SUM(LineTotal) AS 'value'
    FROM
        IGN1
    WHERE
        DocDate BETWEEN {{start_date}} AND {{end_date}}
    GROUP BY
        DocDate
),
all_processing_dates AS (
    SELECT
        as_of_date
    FROM
        to_processing
    UNION
    SELECT
        as_of_date
    FROM
        from_processing
)
SELECT
    all_processing_dates.as_of_date,
    'Processing Movement' AS 'value_category',
    'PROCESSING DIFFERENCE' AS 'value_sub_category',
    COALESCE(to_processing.value, 0) - COALESCE(from_processing.value, 0) AS 'value'
FROM
    all_processing_dates
    LEFT JOIN to_processing ON all_processing_dates.as_of_date = to_processing.as_of_date
    LEFT JOIN from_processing ON all_processing_dates.as_of_date = from_processing.as_of_date
UNION
SELECT
    *
FROM
    to_processing
UNION
SELECT
    *
FROM
    from_processing
UNION
SELECT
    DATEFROMPARTS(
        YEAR(OPCH.DocDate),
        MONTH(OPCH.DocDate),
        DAY(OPCH.DocDate)
    ) AS 'as_of_date',
    'Purchases' AS 'value_category',
    COALESCE(OOND.IndDesc, 'LOCAL SUPPLIERS') AS 'value_sub_category',
    SUM(OPCH.DocTotal) AS 'value'
FROM
    OPCH
    LEFT JOIN OCRD ON OPCH.CardCode = OCRD.CardCode
    LEFT JOIN OOND ON OCRD.IndustryC = OOND.IndCode
WHERE
    OPCH.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND OPCH.CANCELED = 'N'
GROUP BY
    OPCH.DocDate,
    COALESCE(OOND.IndDesc, 'LOCAL SUPPLIERS')
UNION
SELECT
    DATEFROMPARTS(
        YEAR(OPCH.DocDate),
        MONTH(OPCH.DocDate),
        DAY(OPCH.DocDate)
    ) AS 'as_of_date',
    'Purchases' AS 'value_category',
    'TOTAL PURCHASES' AS 'value_sub_category',
    SUM(OPCH.DocTotal) AS 'value'
FROM
    OPCH
    LEFT JOIN OCRD ON OPCH.CardCode = OCRD.CardCode
    LEFT JOIN OOND ON OCRD.IndustryC = OOND.IndCode
WHERE
    OPCH.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND OPCH.CANCELED = 'N'
GROUP BY
    OPCH.DocDate
UNION
SELECT
    DATEFROMPARTS(
        YEAR(ORDR.DocDate),
        MONTH(ORDR.DocDate),
        DAY(ORDR.DocDate)
    ) AS 'as_of_date',
    'Sales' AS 'value_category',
    'SALES' AS 'value_sub_category',
    SUM(ORDR.DocTotal) AS 'value'
FROM
    ORDR
WHERE
    ORDR.DocDate BETWEEN {{start_date}} AND {{end_date}}
    AND ORDR.CANCELED = 'N'
GROUP BY
    DocDate;