with
    cte_source as (
        select * from {{ source("seeds", "BOOKINGSOURCE") }}
    ),

cte_main as (select  

BookingSource as Booking_Source, 
trim(lower(Source)) as Source,
trim(lower(Type)) as Type

 from cte_source )


select * from cte_main