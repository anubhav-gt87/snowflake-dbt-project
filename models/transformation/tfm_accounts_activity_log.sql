with tmp_account_status_change_all AS (
    SELECT 
        account_id, 
        created_ts AS updated_at, 
        'created' AS account_status 
    FROM {{ ref('acq_account_created') }}

    UNION ALL 

    SELECT 
        account_id, 
        closed_ts AS updated_at, 
        'closed' AS account_status 
    FROM {{ ref('acq_account_closed') }}

    UNION ALL 

    SELECT 
        account_id, 
        reopened_ts AS updated_at, 
        'reopened' AS account_status 
    FROM {{ ref('acq_account_reopened') }}
),


-- Create temporary table with status change logs and derived fields
 tmp_account_status_change_log AS (
    with tmp as 
    (SELECT 
        account_id, 
        updated_at AS account_opened_ts,
        date(updated_at) AS account_opened_date, 
        LEAD(updated_at) OVER (PARTITION BY account_id ORDER BY updated_at) AS account_closed_ts,
        LEAD(updated_at) OVER (PARTITION BY account_id ORDER BY updated_at)::date AS account_closed_date,
        account_status               
    FROM tmp_account_status_change_all)

    select * from tmp where account_status <> 'closed'
),

tmp_account_transactions AS (
    select s.*, sum(transactions_num) as num_of_transactions
    from tmp_account_status_change_log s left join {{ref('acq_account_transactions')}} t 
    on s.account_id = t.account_id and 
    t.date >= s.account_opened_date and t.date <= coalesce(s.account_closed_date, '2020-12-31')
    group by 1,2,3,4,5,6
)



select
    f.account_id || '-' || account_opened_ts as unique_row_id,
    f.account_id,
    sac.user_id,
    sac.account_type,
    account_opened_ts,
    account_opened_date,
    account_closed_ts,
    account_closed_date,
    MIN(account_opened_ts) OVER (PARTITION BY f.account_id) AS account_first_created_ts,
    MIN(account_opened_date) OVER (PARTITION BY f.account_id) AS account_first_created_date,
   
    case when ROW_NUMBER() OVER (PARTITION BY f.account_id ORDER BY f.account_opened_ts DESC) = 1 then 'Y' else 'N' end as is_latest,
    case when account_closed_ts is null then 'Y' else 'N' end as is_open,
    case when account_status = 'reopened' then 'Y' else 'N' end as is_reopened,
    case when account_closed_ts is null then 'N' else 'Y' end as is_closed,
    
    num_of_transactions,
    datediff(
        'day',
        date(account_opened_ts),
        coalesce(date(account_closed_ts), current_date())
    ) as account_lifespan_in_days

from tmp_account_transactions f
left join {{ ref('acq_account_created') }} sac 
on f.account_id = sac.account_id
order by f.account_id, account_opened_ts asc

