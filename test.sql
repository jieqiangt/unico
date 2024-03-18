SELECT
    T0.[CardCode],
    T0.[CardName],
    T3.[CardFName],
    T0.[DocNum],
    T0.[DocDate],
    T2.[ItemCode], 
    T2.[ItemName],
    T2.[FrgnName],
    T1.[Quantity],
    T2.[SalUnitMsr] AS 'UoM',
    T1.[GrossBuyPr],
    T1.[Price]
FROM
    ORDR T0
    INNER JOIN RDR1 T1 ON T0.[DocEntry] = T1.[DocEntry]
    INNER JOIN OITM T2 ON T1.[ItemCode] = T2.[ItemCode]
    INNER JOIN OCRD T3 ON T0.[CardCode] = T3.[CardCode]
WHERE
    T0.[DocDate] >= [%0]
    and T0.[DocDate] <= [%1]