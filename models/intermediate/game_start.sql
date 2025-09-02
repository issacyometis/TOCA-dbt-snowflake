with
    cte_source_games as (

        select *
        from {{ ref("stg_gaming__game_status") }}
        where event_type in ('GAME_START')
    ),
    cte_game_end as (select * from {{ ref("game_end") }}),

    cte_tournament_start as (

        select
            t.game_status_id as id,
            t.booking_reference,
            t.venue_id,
            t.game_id,
            concat(t.game_id, t.message_timestamp) as unique_game_id,
            t.message_timestamp as Tournment_StartTime, 
            lead(t.message_timestamp) over (
                partition by t.booking_reference order by t.message_timestamp
            ) as Tournment_EndTime, 
            ROW_NUMBER() over
             (PARTITION BY BOOKING_REFERENCE, Game_ID, game_type, box_id order by
             MESSAGE_TIMESTAMP) as Tournament_Game
        from cte_source_games as t
        where game_id = 'STRIKER' and game_type = 'TOURNAMENT'
    ),

    cte_game_starts_stg as (

        select

            s.game_status_id as id,
            s.booking_reference as booking_reference,
            s.venue_id as venue_id,
            s.box_id as box_id,
            s.game_id as game_id,
            concat(s.game_id, s.message_timestamp) as unique_game_id,
            --iff(
             --   s.game_type = 'TOURNAMENT'
               -- and s.game_id <> lead(s.game_id) over (
                 --   partition by s.booking_reference order by s.message_timestamp
                --),
                --concat(s.game_type, s.game_mode),
                --null
            --) as tournament_id,

            -- IFF(GAME_TYPE = 'TOURNAMENT', concat(GAME_TYPE,ROW_NUMBER() over
            -- (PARTITION BY BOOKING_REFERENCE, Game_ID, game_type, game_mode order by
            -- MESSAGE_TIMESTAMP) ),null) as TOURNAMENT_ID,
            s.game_type as game_type,
            s.game_mode as game_mode,
            s.message_timestamp as game_start_timestamp,
            lead(s.message_timestamp) over (
                partition by s.booking_reference, box_id order by s.message_timestamp
            ) as game_end_timestamp

        from cte_source_games as s
        
    ), 

    cte_main as (
select s.*, 
e.game_end_timestamp as Actual_Game_End, 
E.Game_End_Message_Recieved as Game_Completed, 
IFF(S.game_type='TOURNAMENT', concat(s.game_type, t.Tournament_Game ), null )as TOURNAMENT_ID




from cte_game_starts_stg as S 
left outer join cte_Game_End as E on S.Booking_Reference = E.booking_reference
and s.box_id = E.box_id
and e.Game_End_TimeStamp between s.game_start_timestamp and ifnull(s.game_end_timestamp, dateadd(minute, 20,s.game_start_timestamp))
left outer join cte_tournament_start as T on S.Booking_Reference = t.Booking_Reference and dateadd(second,1,S.game_start_timestamp) between t.Tournment_StartTime 
and ifnull(T.Tournment_EndTime, dateadd(minute, 20,t.Tournment_StartTime)) 


    )

select *
from     cte_main 



