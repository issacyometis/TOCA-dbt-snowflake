
    with
    cte_stg_bookings as (select * from {{ ref("base_sevenrooms__reservations") }}),
    
    cte_box_bookings as (
        select
            reference, 
            replace(replace(table_numbers,'[', ''),']','') as Boxes   
        from cte_stg_bookings
        

    ), 

    cte_main as (

        select distinct reference, B.value::string AS Box_number, iff(Box_number like '%Box%', 1, 0 )as is_box from cte_box_bookings,
        LATERAL FLATTEN(input=>split(Boxes, ',')) B
    )
select *
from cte_main

