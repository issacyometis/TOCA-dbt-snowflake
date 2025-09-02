with
    cte_source_gamesdetail as (select * from {{ ref("fact_game_detail") }}),
    cte_source_games as (select * from {{ ref("games") }}),

    cte_main as (

        select
            gd.sk_site, 
            gd.booking_reference as reference,
            gd.event_date as event_date,
            gd.game_id,
            g.game_name,
            gd.game_mode,
            gd.game_type,
            DATE_PART(  HOUR, gd.message_time ) as Message_Hour,
            gd.box_id,
            gd.difficulty,
            gd.difficulty_name,
            iff(
                gd.is_timeout = 0, 'Shot On Target', 'Missed Shot'
            ) as event_ball_delivered_status,
            sum(iff(gd.is_timeout = 0, 1, 0)) as shots_hit,
            count(gd.game_status_id) as total_shots
        from cte_source_gamesdetail as gd
        left outer join cte_source_games as g on gd.game_id = g.game_id
        where gd.event_type = 'PLAYER_SHOT' and gd.game_id <> 'DEMOMODE'
        group by
            gd.sk_site,
            gd.booking_reference,
            gd.event_date,
            gd.game_id,
            g.game_name,
            gd.game_mode,
            gd.game_type,
            DATE_PART(  HOUR, gd.message_time ) ,
            gd.box_id,
            gd.difficulty,
            gd.difficulty_name,
            iff(gd.is_timeout = 0, 'Shot On Target', 'Missed Shot')

    )

select *
from cte_main
