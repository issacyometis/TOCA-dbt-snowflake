with 
    cte_source as (select * from {{ source('Booking', 'TOCA_BOOKINGS_VENUE') }}),


cte_main as (

select 
ID as Venue_ID
, Name as Venue_Name
, Code as Venue_Code
, schedule_id as Schedule_ID

 

 from cte_source

)

select * from cte_main