WITH orct_calc AS (
    SELECT
        CONVERT(char, DocDate, 112) AS 'doc_date',
        CashSum AS 'cash_sum',
        CreditSum AS 'credit_sum',
        CheckSum AS 'check_sum',
        TrsfrSum AS 'transfer_sum',
        DocTotal AS 'doc_total'
    FROM
        ORCT
    WHERE
        Canceled = 'N'
        AND DocDate BETWEEN {{start_date}} AND {{end_date}}
),
jnl_calc AS (
    SELECT
        CONVERT(char, RefDate, 112) AS 'doc_date',
        0 AS 'cash_sum',
        0 AS 'credit_sum',
        0 AS 'check_sum',
        Debit AS 'transfer_sum',
        Debit AS 'doc_total'
    FROM
        JDT1
    WHERE
        TransType NOT IN (24,46)
        AND RefDate BETWEEN {{start_date}} AND {{end_date}}
		AND DebCred = 'D'
		AND Account IN (11010,11015,11020,11030,11040,11043,11045,11050,11060,11070,11080,11090,11210,11220,11230,11240,11250,11260)
)
SELECT 
    * 
    FROM 
        orct_calc
UNION
SELECT 
    * 
    FROM
        jnl_calc
