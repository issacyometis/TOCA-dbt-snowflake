with
    cte_stg_bookings as (select * from {{ ref("base_sevenrooms__reservations") }}),

    cte_upgrades_bookings as (
        select sevenrooms_reservation_id, reference, upgrades as upgrades
        from cte_stg_bookings

    ),

    cte_main as (

        select
            sevenrooms_reservation_id,
            reference,
            --upgrades,
            value:category_id::string as category_id,
            value:inventory_id::string as inventory_id,
            value:name::string as upgrade_name,
            value:pos_item_id::string as pos_item_id,
            value:price::number/100 as price,
            value:quantity::number as quantity,
            (value:price::number/100) *  value:quantity::number as Total_upgrade

        from cte_upgrades_bookings, lateral flatten(input => upgrades)
    )
select *
from cte_main

