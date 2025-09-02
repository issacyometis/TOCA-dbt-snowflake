with
    cte_source as (select * from {{ source("epos", "TENDER_TYPE_API") }}),

    cte_main as (

        select
            md5(tendertypeid) as tender_type_key,
            tendertypeid as tender_type_id,
            siteid as site_id,
            name as tender_name


        from cte_source
    )

select *
from cte_main
