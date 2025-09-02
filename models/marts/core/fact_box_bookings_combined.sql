with
    cte_stg_bookings as (select * from {{ ref("stg_booking__bookings") }}),
    cte_stg_box_bookings as (
        select * from {{ ref("stg_booking__booking_box_booking") }}
    ),
    cte_sevenroom_boxes as (select * from {{ ref("sevenrooms_boxes") }}),
    cte_stg_bookings_box as (
        select bk.booking_reference as reference, concat('Box ', box.box_id) as box_id

        from cte_stg_bookings as bk
        left outer join cte_stg_box_bookings as box on bk.booking_id = box.booking_id

    ),
    cte_main as (

        select *
        from cte_stg_bookings_box
        union
        select reference, Box_number
        from cte_sevenroom_boxes

    )

select *
from cte_main
