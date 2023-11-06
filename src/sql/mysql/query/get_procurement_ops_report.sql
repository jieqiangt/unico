SELECT
    pdt_code,
    pdt_name,
    profit_margin,
    total_profit,
    profit_per_qty,
    summary_purchases_total_qty,
    summary_sales_total_qty,
    summary_sales_avg_qty_per_month,
    CONCAT(
        summary_purchases_start_date,
        ' - ',
        summary_purchases_end_date
    ) AS 'summary_purchases_date_range',
    summary_purchases_latest_date,
    summary_purchases_latest_price,
    summary_purchases_min_price,
    CONCAT(
        summary_sales_start_date,
        ' - ',
        summary_sales_end_date
    ) AS 'summary_sales_date_range',
    summary_sales_latest_date,
    summary_sales_latest_price,
    summary_sales_max_price,
    summary_sales_median_price,
    summary_sales_min_price
FROM
    ft_pdt_summary;