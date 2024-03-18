SELECT
    IGE1.DocEntry AS 'movement_doc_num',
    DATEFROMPARTS(YEAR(IGE1.DocDate), MONTH(IGE1.DocDate), DAY(IGE1.DocDate)) AS 'doc_date',
    IGE1.ItemCode AS 'pdt_code',
    OITM.ItemName AS 'pdt_name',
    Quantity AS 'qty',
    LineTotal AS 'amount',
    'to_processing' AS 'movement_type'
FROM
    IGE1
    LEFT JOIN OITM ON IGE1.ItemCode = OITM.ItemCode
WHERE
    DocDate BETWEEN {{start_date}} AND {{end_date}}
UNION
SELECT
    IGN1.DocEntry AS 'movement_doc_num',
    DATEFROMPARTS(YEAR(IGN1.DocDate), MONTH(IGN1.DocDate), DAY(IGN1.DocDate)) AS 'doc_date',
    IGN1.ItemCode AS 'pdt_code',
    OITM.ItemName AS 'pdt_name',
    Quantity AS 'qty',
    LineTotal AS 'amount',
    'from_processing' AS 'movement_type'
FROM
    IGN1
    LEFT JOIN OITM ON IGN1.ItemCode = OITM.ItemCode
WHERE
    DocDate BETWEEN {{start_date}} AND {{end_date}};