
select date,
account_id_hashed as account_id,
transactions_num
from dbt_db.dbt_schema.monzo_account_transactions
qualify
    row_number() over (
        partition by account_id, date order by date asc) = 1