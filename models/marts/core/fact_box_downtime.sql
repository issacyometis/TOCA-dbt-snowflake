with
    cte_bookings as (select * from {{ ref("fact_sevenrooms") }}),
    cte_sevenroom_boxes as (select * from {{ ref("sevenrooms_boxes") }}),
    cte_playmaker_pause as (select * from {{ref("game_playmaker_pause")}}),

    cte_boxes as (select reference, Box_number, replace(Box_number, 'Box ', '') as box_id from cte_sevenroom_boxes
    where Box_number like 'Box%'
    ),

    cte_pause as (
            select booking_reference as reference, box_id, sum(paused_in_minutes) as minutes_paused
             from cte_playmaker_pause
             group by booking_reference, box_id

    ),

    box_bookings as (select 
                        book.sk_site, 
                        book.fixture_date,
                        box.box_id::numeric as box_id,
                        --book.reference,
                        book.status,
                        book.status_display,
                        sum(book.Booking_Duration) as box_duration_in_minutes,
                        sum(ifnull(pause.minutes_paused,0)) as minutes_paused,
                        0 as blocked_minutes
     from cte_bookings as Book
     inner join cte_boxes as box on book.reference = box.reference
     left outer join cte_pause as pause on box.reference = pause.reference and box.box_id = pause.box_id
     group by book.sk_site, 
                book.fixture_date,
                box.box_id,
                book.status,
                book.status_display
     )



    select *  from box_bookings