with
    cte_source as (

        {{
            dbt_utils.snowflake__deduplicate(
                relation=ref('stg_people_counter__people_count_by_zone_api'),
                partition_by='zone_id, start_date, start_time',
                order_by='hevo_ingested_at desc',
            )
        }}

    ),

    cte_main as (

        select 
            customer_id,
            zone_id,
            zone_name,
            start_date,
            end_date,
            start_time,
            end_time,
            business_date_start,
            count_in,
            count_out,
            hevo_ingested_at,
            hevo_loaded_at            
        from cte_source
    )

select *
from cte_main