with cte_stg_bookings as (

select * from {{ref('stg_booking__bookings')}}
) , 


cte_main as (

select 
Distinct 
MD5(coalesce(Booking_Type,'N/A')) as Booking_Type_Key, 
coalesce(Booking_Type,'N/A') as Booking_Type 
from cte_stg_bookings

)


select * from cte_main