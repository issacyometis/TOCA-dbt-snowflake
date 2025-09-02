with 
    cte_source as (select * from {{ source('Booking', 'TOCA_BOOKINGS_BOX_BOOKING') }}),

cte_main as (

select 
ID as Box_Booking_ID
, Booking_ID 
, BOX_ID


 from 
cte_source
)


select * from cte_main
