SELECT
    pdt_code,
    CONCAT(
        summary_purchases_start_date,
        ' - ',
        summary_purchases_end_date
    ) AS 'purchase_data_date_range',
    summary_purchases_latest_date AS 'latest_purchase_date',
    summary_purchases_latest_price AS 'latest_purchase_price',
    summary_purchases_min_price AS 'min_purchase_price',
    summary_purchases_max_price AS 'max_purchase_price',
    summary_purchases_avg_qty_per_month AS 'avg_monthly_purchase_qty',
    CONCAT(
        summary_sales_start_date,
        ' - ',
        summary_sales_end_date
    ) AS 'sales_data_date_range',
    summary_sales_latest_date AS 'latest_sales_date',
    summary_sales_latest_price AS 'latest_sales_price',
    summary_sales_min_price AS 'min_sales_price',
    summary_sales_max_price AS 'max_sales_price',
    summary_sales_avg_qty_per_month AS 'avg_monthly_sales_qty',
    last_7_days_sales_is_active AS 'had_sales_last_7_days',
    year_to_date_sales_qty,
    year_to_date_purchases_qty
FROM
    ft_pdt_summary;