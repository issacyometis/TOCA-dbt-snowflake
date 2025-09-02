with
    cte_source as (
        select * from {{ source("inputs", "TOCA_BUSINESS_DRIVERS_SHEET1") }}
    ),

    cte_main as (
        select

            toca_reporting_id,
            epos_id::numeric as epos_id,
            gaming_venue_id,
            sevenrooms_id,
            peoplecounter::numeric(10,0) as people_counter_id,
            site_name,
            post_code,
            googleformid as google_form_id,
            sentiment_site_name,
            BudgetCode as budget_code

        from cte_source
    )

select *
from cte_main
