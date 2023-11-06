SELECT
   pdt_code,
   summary_purchases_order_count,
   summary_purchases_latest_date,
   summary_purchases_latest_price,
   summary_purchases_min_price,
   summary_purchases_25_percentile_price,
   summary_purchases_avg_price,
   summary_purchases_median_price,
   summary_purchases_75_percentile_price,
   summary_purchases_max_price,
   COALESCE(summary_upper_purchases_price_iqr_limit, 999999) AS 'summary_upper_purchases_price_iqr_limit',
   COALESCE(summary_lower_purchases_price_iqr_limit, -999999) AS 'summary_lower_purchases_price_iqr_limit',
   COALESCE(summary_upper_purchases_price_std_limit, 999999) AS 'summary_upper_purchases_price_std_limit',
   COALESCE(summary_lower_purchases_price_std_limit, -999999) AS 'summary_lower_purchases_price_std_limit'
FROM
   ft_pdt_summary;


