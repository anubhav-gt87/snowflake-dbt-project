-- Acquisition table for source - monzo_account_created
{{ config(materialised="view") }}

with
    acq_accunt_created as (
        select
            to_timestamp_tz(replace(created_ts, ' UTC', ' +00:00')) as created_ts,
            account_type,
            account_id_hashed as account_id,
            user_id_hashed as user_id
        from dbt_db.dbt_schema.monzo_account_created
    )
select *
from acq_accunt_created
qualify row_number() over (partition by account_id order by created_ts desc) = 1
