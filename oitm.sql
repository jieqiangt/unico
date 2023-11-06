SELECT
    CONVERT(char, OITM.CreateDate, 112) AS 'create_date',
	CONVERT(char, OITM.UpdateDate, 112) AS 'update_date',
    ItemCode AS 'item_code',
    ItemName AS 'item_name',
    ItmsGrpNam AS 'item_category',
    (
        CASE
            WHEN PrchseItem = 'N' THEN 0
            ELSE 1
        END
    ) AS 'purchase_item_ind',
    (
        CASE
            WHEN SellItem = 'N' THEN 0
            ELSE 1
        END
    ) AS 'sales_item_ind',
    (
        CASE
            WHEN InvntItem = 'N' THEN 0
            ELSE 1
        END
    ) AS 'inv_item_ind',
    BuyUnitMsr AS 'purchase_uom',
    SalUnitMsr AS 'sales_uom',
    TreeType AS 'bom_type',
    (
        CASE
            WHEN validFor = 'N' THEN 0
            ELSE 1
        END
    ) AS 'active_ind',
    (
        CASE
            WHEN frozenFor = 'N' THEN 0
            ELSE 1
        END
    ) AS 'inactive_ind',
    SWW AS 'additional_identifier',
    ItemType AS 'item_type',
    InvntryUom AS 'inv_uom',
    OMTP.MatType AS 'mat_type',
    OMTP.Descrip AS 'mat_type_desc',
    OMGP.MatGrp AS 'mat_grp',
    OMGP.Descrip AS 'mat_grp_desc',
    OPSC.[Desc] AS 'product_src_desc',
    OSCG.ServiceCtg AS 'service_cat',
    OSCG.Descrip AS 'service_cat_desc',
    (
        CASE
            WHEN OITM.ItemClass = 1 THEN 0
            ELSE 1
        END
    ) AS 'material_ind',
    (
        CASE
            WHEN Excisable = 'N' THEN 0
            ELSE 1
        END
    ) AS 'excisable_ind'
FROM
    OITM
    LEFT JOIN OITB ON OITM.ItmsGrpCod = OITB.ItmsGrpCod
    LEFT JOIN OMTP ON OITM.MatType = OMTP.AbsEntry
    LEFT JOIN OMGP ON OITM.MatGrp = OMGP.AbsEntry
    LEFT JOIN OPSC ON OITM.ProductSrc = OPSC.Code
    LEFT JOIN OSCG ON OITM.ServiceCtg = OSCG.AbsEntry;