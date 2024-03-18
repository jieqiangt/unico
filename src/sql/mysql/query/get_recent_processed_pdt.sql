SELECT
    DISTINCT pdt_code AS 'pdt_code'
FROM
    ft_processed_pdt_daily_output_ts
WHERE doc_date BETWEEN {{start_date}} AND {{end_date}}
AND qty > 0;