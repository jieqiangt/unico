SELECT
    {{as_of_date}} AS 'as_of_date',
    OITM.ItemCode AS 'pdt_code',
    OITM.SalUnitMsr AS 'uom',
    OITW.WhsCode AS 'warehouse_code',
    OITW.OnHand AS 'on_hand',
    OITW.IsCommited AS 'is_committed',
    OITW.OnOrder AS 'on_order',
    OITW.Consig AS 'consig',
    OITW.Counted AS 'counted',
    (
        CASE
            WHEN OITW.WasCounted = 'N' THEN 0
            ELSE 1
        END
    ) AS 'was_counted'
FROM
    OITM
    LEFT JOIN OITW ON OITM.ItemCode = OITW.ItemCode;