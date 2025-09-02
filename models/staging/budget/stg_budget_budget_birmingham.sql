with
    cte_source as (select * from {{ source("budget", "BUDGETS_BIRM2024") }}),
    cte_main as (

        select


            2 as site_ID,
            to_date(date) as Budget_date,
            Cast(hours as int) as box_hours,
            Cast(Replace(Replace(box_rev, '£', ''), ',', '') as int) as game_rev,
            Cast(Replace(Replace(food_rev, '£', ''), ',', '') as int) as food,
            Cast(Replace(Replace(drinks_rev, '£', ''), ',', '') as int) as drinks,
            Cast(0 as int) as sweet_finish,
           Cast(0 as int) as merch,
           Cast(0 as int)  as people,
            Cast(0 as int)  as bookings 



        from cte_source
       -- where  to_date(date)<>'2024-12-25'
    -- where len(date) = 10
    )

select *

from cte_main

