with
    cte_source as (
        select * from {{ source("people_counter", "PEOPLE_COUNT_BY_ZONE_API") }}
    ),

    cte_main as (

        select 
            customer_id,
            zone_id,
            zone_name,
            to_date(starttime) as start_date,
            to_date(endtime) as end_date,
            to_time(starttime) as start_time,
            to_time(endtime) as end_time,
            case
                when to_time(starttime) < '05:00:00'
                then to_date(starttime) - 1
                else to_date(starttime)
            end as business_date_start,
            count_in::numeric(10,0) as count_in,
            count_out::numeric(10,0) as count_out,
            to_timestamp(__HEVO__INGESTED_AT::number / 1000) as hevo_ingested_at,
            to_timestamp(__HEVO__LOADED_AT::number / 1000) as hevo_loaded_at
            
        from cte_source

    )

select *
from cte_main
