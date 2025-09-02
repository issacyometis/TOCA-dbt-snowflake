with cte_stg_bookings as (

select * from {{ref('stg_input__booking_source')}}
) , 


cte_main as (

select 
Distinct 
MD5(coalesce(Booking_Source,'N/A')) as Booking_Source_Key, 
coalesce(Booking_Source,'N/A') as Booking_Source
from cte_stg_bookings

)


select * from cte_main