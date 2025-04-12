with
    acq_account_closed as (
        select
            to_timestamp_tz(replace(closed_ts, ' UTC', ' +00:00')) as closed_ts,
            account_id_hashed as account_id
        from dbt_db.dbt_schema.monzo_account_closed
    )

select *
from acq_account_closed
qualify
    row_number() over (
        partition by account_id, date(closed_ts) order by closed_ts desc
    ) = 1
    -- if there are multiple closing timestamp for an account on the same day, taking
    -- the latest record in that case
    
