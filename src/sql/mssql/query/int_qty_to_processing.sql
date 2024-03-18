SELECT
    IGE1.DocDate AS 'doc_date',
    IGE1.ItemCode AS 'pdt_code',
    Quantity AS 'processed_qty'
FROM
    IGE1
    LEFT JOIN OITM ON IGE1.ItemCode = OITM.ItemCode
WHERE
    DocDate BETWEEN {{start_date}}
    AND {{end_date}};