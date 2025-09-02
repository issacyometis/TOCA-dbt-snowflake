with
    cte_stg_venue as (select * from {{ ref("stg_inputs__business_driver") }}),

    cte_main as (
            select 
                md5(toca_reporting_id) as sk_site, 
                * 
            from cte_stg_venue)

select *
from cte_main
