with cte_stg_venue as (

select * from {{ref('stg_booking__booking_venue')}}
) , 


cte_main as (

select 
Venue_id as Location_Key
,Venue_ID as Location_ID
,Venue_Name 
,Venue_Code




from cte_stg_venue

)


select * from cte_main