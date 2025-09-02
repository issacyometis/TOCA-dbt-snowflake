with
    cte_source_game_rounds as (

        select *
        from {{ ref("stg_gaming__game_status") }}
        where event_type in ('ROUND_START')
    ),
    cte_game_rounds as (

        select

            game_status_id as id,
            booking_reference as booking_reference,
            venue_id as venue_id,
            box_id as box_id,
            game_id as game_id,
            current_round as current_round,
            number_rounds as number_rounds,
            message_timestamp as round_start_timestamp,
            lead(message_timestamp) over (
                partition by booking_reference, box_id order by message_timestamp
            ) as round_end_timestamp
        from cte_source_game_rounds

    )


select *
from cte_game_rounds
