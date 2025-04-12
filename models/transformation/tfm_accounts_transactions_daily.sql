
with tmp_account_date_spine as 
(select t.account_id, user_id, account_type, account_first_created_ts, account_first_created_date,
cal_date from 
{{ref('tfm_accounts_activity_log')}} t 
cross join {{ref('date_dim')}} where  
cal_date >= account_opened_date 
AND cal_date <= coalesce(account_closed_date, '2020-12-31')  
)


select t.*, 
coalesce(f.transactions_num,0) as num_transactions, 
SUM(COALESCE(f.transactions_num, 0)) 
        OVER (PARTITION BY t.account_id ORDER BY t.cal_date 
              ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS num_transaction_last_7d
from 
tmp_account_date_spine t
left join {{ref('acq_account_transactions')}} f on 
t.account_id = f.account_id and 
t.cal_date = f.date



/*

WITH tmp_account_status_daily AS (
    SELECT 
        account_id,
        account_type, 
        user_id,
        DATE(created_ts) AS account_created_date, 
        cal_date AS date
    FROM 
        {{ref('acq_account_created')}} 
    CROSS JOIN 
        {{ref('date_dim')}} 
    WHERE 
        cal_date >= DATE(created_ts) 
        AND cal_date <= '2020-12-31'  -- current_date()
)

SELECT 
    ad.*,
    CASE 
        WHEN ast.account_id IS NOT NULL THEN 'Y' 
        ELSE 'N' 
    END AS is_acc_active, 
    COALESCE(atr.transactions_num, 0) AS transactions_num,
    SUM(COALESCE(atr.transactions_num, 0)) 
        OVER (PARTITION BY ad.account_id ORDER BY ad.date 
              ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS num_transaction_last_7d
FROM 
    tmp_account_status_daily ad
LEFT JOIN 
    {{ref('tfm_accounts_activity_log')}} ast 
    ON ad.account_id = ast.account_id 
    AND ad.date >= ast.account_opened_date 
    AND ad.date <= COALESCE(ast.account_closed_date, CURRENT_DATE())
LEFT JOIN 
     {{ref('acq_account_transactions')}}  atr 
    ON ad.account_id = atr.account_id_hashed 
    -- atr.date >= ast.account_opened_date and atr.date <= COALESCE(ast.account_closed_date, '2020-12-31')
    AND ad.date = atr.date
ORDER BY 
    ad.user_id, ad.account_id, ad.date ASC
*/