with
    cte_stg_payout as (
        select * from {{ ref("stg_stripe__payout") }}
    ),
    

    cte_main as (

        select
        created_date as Payout_date,
        created_time as Payout_time,
        arrival_date,
        automatic,
        currency,
        description,
        payout_transaction_id,
        livemode,
        method, 
        Source_type,
        Status,
        Type,
        balance_transaction_id,
        Amount

            
        from cte_stg_payout as b
        

    )

select *
from cte_main
