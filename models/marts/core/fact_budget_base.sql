with

    cte_stg_budget as (select * from {{ ref("stg_base_budgets") }}),
    cte_business_drivers as (select * from {{ ref("dim_business_drivers") }}),
    cte_stg as (

        select *
        from
            cte_stg_budget unpivot (
                budget for budget_type in (
                    total_revenue,
                    box_hours,
                    game_rev,
                    food,
                    drinks,
                    sweet_finish,
                    merch,
                    people,
                    bookings
                )
            )
    ),

    cte_main as (
        select b.sk_site, s.*
        from cte_stg as s
        left outer join cte_business_drivers as b on s.site_id = b.budget_code
    )

select *
from cte_main
