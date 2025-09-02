with
    cte_stg_bookings as (select * from {{ ref("base_sevenrooms__reservations") }}),
cte_stg_payments as (select * from {{ ref("base_sevenrooms__charges") }}),
    cte_main as (

        select
P.*, 
P.amount/100 as amount_pounds,
b.reference
      
        from cte_stg_payments as p
        left outer join cte_stg_bookings as b on P.Booking_id = b.Sevenrooms_reservation_id


    )

select *
from cte_main
