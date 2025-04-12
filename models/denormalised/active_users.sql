select cal_date, 
count(*) as num_of_active_users, 
sum(num_of_active_accounts) as num_of_active_accounts,
sum(num_transactions) as num_transactions,
sum(case when num_transaction_last_7d > 0 then 1 else 0 end) as `7d_active_users`,
sum(case when num_transaction_last_7d > 0 then 1 else 0 end)/count(*) as `7d_active_users_perc`
from {{ref('tfm_users_transactions_daily')}}
group by 1 
order by 1