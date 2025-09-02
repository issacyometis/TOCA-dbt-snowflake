with
    cte_stg_bookings as (select * from {{ ref("base_sevenrooms__reservations") }}),
    cte_site as (select * from {{ ref("dim_business_drivers") }}),
    cte_tags_bookings as (
        select s.sk_site, sevenrooms_reservation_id, reference, fixture_at, tags as tags
        from cte_stg_bookings as b
        left outer join cte_site as s on b.Venue_ID = s.SEVENROOMS_ID

    ),
   

    cte_main as (

        select
            sk_site,
            fixture_at,
            sevenrooms_reservation_id,
            reference,
            value:group::string as group_name,
            value:group_display::string as group_display,
            value:tag::string as tag,
            value:tag_display::string as tag_display,
            value:color as color,
            value:is_autotag as is_autotag

        from cte_tags_bookings, lateral flatten(input => tags)
        
    )
select *
from cte_main



