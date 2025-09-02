with
    cte_source as (select * from {{ source("budget", "BUDGETS_LIVE_BUDGETS") }}),
    cte_main as (

        select


            site_code as site_id, 
            total_revenue::numeric(8,2) as total_revenue,
            game_rev::numeric(8,2) as game_rev,
            food_rev::numeric(8,2) as food,
            drinks_rev::numeric(8,2) as drinks,
            hours::numeric(8,2) as box_hours,
            merch_other::numeric(8,2) as merch, 
            players::numeric(8,2) as people,
            0::numeric(8,2) as sweet_finish, 
            0::numeric(8,2) as bookings,
            
            to_date(business_date) as Budget_date,

        from cte_source

    )

select *

from cte_main

