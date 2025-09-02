with
    cte_source as (select * from {{ source("stripe", "refund") }}),

    cte_main as (
        select
            to_date(to_timestamp_tz(c.created)) as Created_date,
            to_time(to_timestamp_tz(c.created)) as Created_time,
            currency ,
            id as refund_transaction_id,
            status,
            balance_transaction_id,
            charge_id,
            metadata_reason,
            reason,
            receipt_number,
            amount / 100 as amount

        from cte_source as c
    )

select *
from cte_main
