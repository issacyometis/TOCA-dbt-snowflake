with 
    cte_source as (select * from {{ source('Booking', 'TOCA_BOOKINGS_BOX') }}),


cte_main as (

select 
concat(Venue_ID,'-', ID) as Box_Unique_ID,
ID as BOX_ID,
concat('BOX - ' , right(concat('00' ,name),2)) as Box_Name, 
Venue_ID,
Section as Section_ID,
concat('Section - ' , Section) as Section

 from cte_source

)

select * from cte_main