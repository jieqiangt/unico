SELECT
    dim_pdts.pdt_code,
    dim_pdts.pdt_name,
    dim_pdts.uom,
    COALESCE(on_hand_qty,0) AS 'on_hand_qty',
    COALESCE(on_hand_value,0) AS 'on_hand_value',
    COALESCE(is_commited_qty,0) AS 'is_commited_qty',
    COALESCE(is_commited_value,0) AS 'is_commited_value',
    COALESCE(on_order_qty,0) AS 'on_order_qty',
    COALESCE(on_order_value,0) AS 'on_order_value',
    COALESCE(consig_qty,0) AS 'consig_qty',
    COALESCE(consig_value,0) AS 'consig_value',
    COALESCE(summary_sales_avg_qty_per_month, 0) AS 'monthly_sales_qty',
    COALESCE((on_hand_qty + is_commited_qty) / summary_sales_avg_qty_per_month, 0) AS 'monthly_sales_qty_to_current_inv_ratio',
    last_7_days_sales_is_active AS 'last_7_days_sales_ind',
    CASE
        WHEN COALESCE(summary_sales_avg_qty_per_month / (on_hand_qty + is_commited_qty), 0) >= 4 THEN 'SLOW SALES'
        ELSE 'NORMAL'
    END AS 'slow_sales_ind',
    dim_pdts.new_pdt_ind
FROM
    dim_pdts
    LEFT JOIN ft_current_warehouse_inv_breakdown ON dim_pdts.pdt_code = ft_current_warehouse_inv_breakdown.pdt_code
    LEFT JOIN ft_pdt_summary ON dim_pdts.pdt_code = ft_pdt_summary.pdt_code;