with
    cte_source as (select * from {{ source("inputs", "end_of_day") }}),
    cte_main as (

        select

*


        from cte_source
   
    )

select *
from cte_main
