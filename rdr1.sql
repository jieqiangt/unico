WITH DocNumTbl AS (
    SELECT
        DISTINCT DocEntry,
        BaseRef
    FROM
        INV1
    WHERE
        BaseRef IS NOT NULL
)
SELECT
    CONVERT(char, RDR1.DocDate, 112) AS 'doc_date',
    RDR1.DocEntry AS 'id',
    RDR1.LineNum AS 'line_num',
    TargetType AS 'downstream_doc_type',
    TrgetEntry AS 'inv1_id',
    BaseType AS 'base_doc_type',
    DocNumTbl.BaseRef AS 'ordr_doc_no',
    (
        CASE
            WHEN LineStatus = 'C' THEN 'closed'
            ELSE 'open'
        END
    ) AS 'line_status',
    RDR1.ItemCode AS 'item_code',
    OITM.ItemName AS 'item_description',
    ShipDate AS 'delivery_date',
    Quantity AS 'quantity',
    Price AS 'price',
    LineTotal AS 'total_before_vat',
    LineVat AS 'vat_value',
    GTotal AS 'total',
    OSLP.SlpName AS 'sales_agent',
    OWHS.WhsCode AS 'warehouse_code',
    OWHS.WhsName AS 'warehouse_name',
    RDR1.TreeType AS 'bom_type',
    RDR1.BaseCard AS 'customer_code',
    OCRD.CardName AS 'customer_name',
    OCRD.Address AS 'customer_address',
    OCRD.ZipCode AS 'customer_zip_code',
    RDR1.SWW AS 'additional_identifier',
    COALESC E(unitMsr, unitMsr2) AS 'uom',
    ShipToCode AS 'customer_to_ship_to',
    ShipToDesc AS 'customer_address_to_ship_to',
    (
        CASE
            WHEN DescOW = 'N' THEN 0
            ELSE 1
        END
    ) AS 'desc_overwritten_ind',
    (
        CASE
            WHEN DetailsOW = 'N' THEN 0
            ELSE 1
        END
    ) AS 'details_overwritten_ind'
FROM
    RDR1
    LEFT JOIN DocNumTbl ON RDR1.TrgetEntry = DocNumTbl.DocEntry
    LEFT JOIN OCRD ON RDR1.BaseCard = OCRD.CardCode
    LEFT JOIN OITM ON RDR1.ItemCode = OITM.ItemCode
    LEFT JOIN OSLP ON RDR1.SlpCode = OSLP.SlpCode
    LEFT JOIN OWHS ON RDR1.WhsCode = OWHS.WhsCode