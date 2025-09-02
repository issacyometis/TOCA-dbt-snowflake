with cte_stg_box as (

select * from {{ref('stg_booking__booking_box')}}
) , 


cte_main as (

select 

MD5(box_unique_ID) as Box_key, 
*

 from cte_stg_box

)

select * from cte_main