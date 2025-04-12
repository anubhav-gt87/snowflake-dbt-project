with
    acq_account_reopened as (
        select
            to_timestamp_tz(replace(reopened_ts, ' UTC', ' +00:00')) as reopened_ts,
            account_id_hashed as account_id
        from dbt_db.dbt_schema.monzo_account_reopened
    )
select *
from acq_account_reopened
qualify
    row_number() over (
        partition by account_id, date(reopened_ts) order by reopened_ts desc) = 1
