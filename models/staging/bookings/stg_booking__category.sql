with 
    cte_source as (select * from {{ source('Booking', 'TOCA_PACKAGES_CATEGORY') }}),


cte_main as (

select 

ID As Category_ID,
NAME as Category_Name,
Priority as Priority
 from cte_source

)

select * from cte_main