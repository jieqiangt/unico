DELETE FROM
    {{table}}
WHERE
    {{date_col}} BETWEEN {{start_date}} AND {{end_date}}