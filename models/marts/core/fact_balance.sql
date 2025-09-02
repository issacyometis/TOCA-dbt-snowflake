with
    cte_stg_balance_transactions as (
        select * from {{ ref("stg_stripe__balance_transactions") }}
    ),
    cte_stg_balance_charge as (select * from {{ ref("stg_stripe__charge") }}),
    cte_stg_refund as (select * from {{ ref("stg_stripe__refund") }}),

    cte_main as (

        select

            b.created_date,
            b.created_time,
            b.available_date,
            b.available_time,
            c.created_date as charge_date,
            c.created_time as charge_time,
            r.created_date as refund_date,
            r.created_time as refund_time,
            b.status,
            b.type as balance_type,
            b.Transaction_id,
            c.metadata_booking_ref as booking_reference,
            c.metadata_session_id as session_id,
            r.refund_transaction_id,
            c.paid,
            c.refunded,
            ifnull(b.fee,0) as Fee,
            ifnull(b.net,0) as Net ,
            ifnull(b.amount,0) as Amount,
            ifnull(r.amount,0)  as amount_refunded,
            ifnull(c.amount_refunded,0)  as charge_refund, 
            ifnull(c.application_fee_amount,0)  as application_fee_amount,
            ifnull(c.amount_captured,0) as Amount_Captured

        from cte_stg_balance_transactions as b
        left outer join
            cte_stg_balance_charge as c on b.source_id = c.charge_transaction_id
        left outer join cte_stg_refund as r on b.source_id = r.refund_transaction_id

    )

select *
from cte_main
