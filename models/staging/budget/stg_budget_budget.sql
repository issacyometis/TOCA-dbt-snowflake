with
    cte_source as (select * from {{ source("budget", "DAILY_BUDGETS") }}),
    cte_main as (

        select


            1 as site_ID,
            to_date(date) as Budget_date,
            Cast(boxhours as int) as box_hours,
            Cast(Replace(Replace(game_rev, '£', ''), ',', '') as int) as game_rev,
            Cast(Replace(Replace(food, '£', ''), ',', '') as int) as food,
            Cast(Replace(Replace(drinks, '£', ''), ',', '') as int) as drinks,
            Cast(Replace(Replace(sweet_finish, '£', ''), ',', '') as int) as sweet_finish,
            Cast(Replace(Replace(merch, '£', ''), ',', '')as int) as merch,
            Cast(Replace(Replace(people, '£', ''), ',', '')as int)  as people,
            Cast(Replace(Replace(bookings, '£', ''), ',', '')as int)  as bookings 



        from cte_source
        where  to_date(date)<>'2024-12-25'
    -- where len(date) = 10
    )

select *

from cte_main

