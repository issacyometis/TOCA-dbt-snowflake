with
    cte_stg_tender_type as (select * from {{ ref("stg_epos__tender_type") }}),


    cte_main as (select * from cte_stg_tender_type)


select *
from cte_main
