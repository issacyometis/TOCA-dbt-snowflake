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
            date_part(hour, gd.message_time) as message_hour,
            gd.box_id,
            gd.difficulty,
            gd.difficulty_name,
            gd.player_name,
            gd.contact,
            gd.is_gdpr_accepted,
            gd.is_gdpr_underage,
            gd.totalgametime as seconds_to_complete,
            count(gd.game_status_id) as toca_challenge_completed
        from cte_source_gamesdetail as gd
        left outer join cte_source_games as g on gd.game_id = g.game_id
        where gd.is_toca_win = 1
        group by
            gd.sk_site, 
            gd.booking_reference,
            gd.event_date,
            gd.game_id,
            g.game_name,
            gd.game_mode,
            gd.game_type,
            date_part(hour, gd.message_time),
            gd.box_id,
            gd.difficulty,
            gd.difficulty_name,
            gd.player_name,
            gd.contact,
            gd.is_gdpr_accepted,
            gd.is_gdpr_underage,
            gd.totalgametime
    )

select *
from cte_main
