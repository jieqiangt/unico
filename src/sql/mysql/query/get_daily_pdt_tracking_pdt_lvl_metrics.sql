SELECT
    dim_pdts.pdt_code,
    dim_pdts.pdt_name,
    dim_pdts.uom,
    COALESCE(current_inv,0) AS 'current_inv',
    COALESCE(summary_sales_avg_qty_per_month, 0) AS 'monthly_sales_qty',
    COALESCE(current_inv / summary_sales_avg_qty_per_month, 0) AS 'monthly_sales_qty_to_current_inv_ratio',
    last_7_days_sales_is_active AS 'last_7_days_sales_ind',
    CASE
        WHEN COALESCE(summary_sales_avg_qty_per_month / current_inv, 0) >= 4 THEN 'SLOW SALES'
        ELSE 'NORMAL'
    END AS 'slow_sales_ind',
    dim_pdts.new_pdt_ind
FROM
    dim_pdts
    LEFT JOIN ft_current_inv_value ON dim_pdts.pdt_code = ft_current_inv_value.pdt_code
    LEFT JOIN ft_pdt_summary ON dim_pdts.pdt_code = ft_pdt_summary.pdt_code;