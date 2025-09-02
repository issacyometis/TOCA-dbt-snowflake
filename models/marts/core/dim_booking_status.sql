with cte_stg_bookings as (

select * from {{ref('stg_booking__bookings')}}
) , 


cte_main as (

select 
Distinct 
MD5(coalesce(Booking_status,'N/A')) as booking_Status_Key, 
coalesce(Booking_status,'N/A') as booking_Status 
from cte_stg_bookings

)


select * from cte_main