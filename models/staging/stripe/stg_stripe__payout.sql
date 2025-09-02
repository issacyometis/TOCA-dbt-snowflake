with
    cte_source as (select * from {{ source("stripe", "payout") }}),

    cte_main as (
        select
            to_date(to_timestamp_tz(c.created)) as Created_date,
            to_time(to_timestamp_tz(c.created)) as Created_time,
            to_date(to_timestamp_tz(c.arrival_date)) as arrival_date,
            to_time(to_timestamp_tz(c.arrival_date)) as arrival_time,
            automatic,
            currency,
            description,
            id as payout_transaction_id,
            livemode,
            method,
            source_type,
            status,
            type,
            balance_transaction_id,
            amount / 100 as amount

        from cte_source as c
    )

select *
from cte_main
