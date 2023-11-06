SELECT
    supplier_code,
    name AS 'supplier_name',
    overseas_local_ind,
    trade_ind,
    is_active
FROM
    dim_suppliers