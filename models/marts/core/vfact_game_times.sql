with
    cte_source_fact_game_detail as (select * from {{ ref("fact_game_detail") }}),

    cte_game_start_end as (

        select
            sk_site,
            booking_reference,
            box_id, 
            unique_game_id,
            tournament_id,
            game_id,
            event_date,
            event_time,
            game_type,
            min(iff(event_type = 'GAME_START', message_time, null)) as game_start,
            max(iff(event_type = 'GAME_END', message_time, null)) as game_end,
            count(distinct player_id) as no_of_players,
            timediff(
                minute,
                min(iff(event_type = 'GAME_START', message_time, null)),
                max(iff(event_type = 'GAME_END', message_time, null))
            ) as game_minutes,
            concat(Message_Date,'T',min(iff(event_type = 'GAME_START', message_time, null))) as Game_Start_DateTime, 
            concat(Message_Date,'T',max(iff(event_type = 'GAME_END', message_time, null))) as Game_End_DateTime

        from cte_source_fact_game_detail
        -- where event_type in ('GAME_START', 'GAME_END') 
        group by
            sk_site, 
            booking_reference,
            box_id, 
            unique_game_id,
            tournament_id,
            event_date,
            event_time,
            game_type,
            game_id, message_date
    ),

    cte_complete_tornement as (

        select booking_reference, box_id, tournament_id, event_date,  1 as is_tournament_complete
        from cte_game_start_end
        where Game_ID = 'ELIMINATOR' and tournament_id is not null and game_end is not null 

    ), 

    cte_main as (

        select g.*, T.is_tournament_complete from cte_game_start_end as G
        Left outer join cte_complete_tornement as T on g.Booking_Reference = t.Booking_Reference and g.tournament_id = t.tournament_id and t.box_id = g.box_id
    )

select *
from cte_main
--WHERE BOOKING_REFERENCE = '344J5F72D'
