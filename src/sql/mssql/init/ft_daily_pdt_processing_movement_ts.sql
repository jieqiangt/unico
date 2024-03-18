WITH to_processing AS (
    SELECT
        DATEFROMPARTS(
            YEAR(IGE1.DocDate),
            MONTH(IGE1.DocDate),
            DAY(IGE1.DocDate)
        ) AS 'as_of_date',
        SUM(Quantity) AS 'qty_to_processing',
        SUM(LineTotal) AS 'value_to_processing'
    FROM
        IGE1
    WHERE
        DocDate BEtWEEN {{start_date}}AND {{end_date}}
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
        SUM(Quantity) AS 'qty_from_processing',
        SUM(LineTotal) AS 'value_from_processing'
    FROM
        IGN1
    WHERE
        DocDate BEtWEEN {{start_date}}AND {{end_date}}
    GROUP BY
        DocDate
)
SELECT
    to_processing.as_of_date,
    COALESCE(qty_to_processing,0) AS 'qty_to_processing',
    COALESCE(qty_from_processing,0) AS 'qty_from_processing',
    COALESCE(value_from_processing,0) AS 'value_from_processing',
    COALESCE(value_to_processing,0) AS 'value_to_processing'
FROM
    to_processing
    LEFT JOIN from_processing ON to_processing.as_of_date = from_processing.as_of_date 