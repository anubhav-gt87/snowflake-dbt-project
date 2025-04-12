select user_id, 
cal_date, 
count(distinct account_id) as num_of_active_accounts,
sum(num_transactions) as num_transactions,
sum(num_transaction_last_7d) as num_transaction_last_7d
from {{ref('tfm_accounts_transactions_daily')}}
 group by 1,2 
 order by 1,2