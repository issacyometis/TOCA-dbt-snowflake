with
    cte_source as (select * from {{ source("budget", "BUDGETS_BASEBUDGETS") }}),
    cte_main as (

        select


            replace(site_code, '_Base', '') as site_id, 
            coalesce(nullif(total_revenue, '-'),0)::numeric(8,2) as total_revenue,
            coalesce(nullif(game_rev, '-'),0)::numeric(8,2) as game_rev,
            coalesce(nullif(food_rev, '-'),0)::numeric(8,2) as food,
            coalesce(nullif(drinks_rev, '-'),0)::numeric(8,2) as drinks,
            0::numeric(8,2) as box_hours,
            coalesce(nullif(merch_other, '-'),0)::numeric(8,2) as merch, 
            coalesce(nullif(players, '-'),0)::numeric(8,2) as people,
            0::numeric(8,2) as sweet_finish, 
            0::numeric(8,2) as bookings,
            
            to_date(business_date) as Budget_date,

        from cte_source

    )

select *

from cte_main

