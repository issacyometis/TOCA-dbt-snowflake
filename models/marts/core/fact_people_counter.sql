with 
    cte_people_counter as (select * from {{ref('base_people_counter__people_count_by_zone_api')}}),
    cte_business_drivers as (select * from {{ref("dim_business_drivers")}}),


    cte_main as (
        select 
            bd.sk_site,
            pc.business_date_start,
            pc.start_date,
            pc.start_time,
            pc.end_date,
            pc.end_time,
            pc.count_in,
            pc.count_out
        from cte_people_counter as pc
        join cte_business_drivers as bd
            on pc.zone_id = bd.people_counter_id
    )

select * from cte_main