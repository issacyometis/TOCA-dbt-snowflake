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
            upper(power_up_type) as power_up_type,
            sum(power_up_displayed) as power_up_displayed,
            sum(power_up_hit) as power_up_hits,
            count(gd.game_status_id) as total_shots
        from cte_source_gamesdetail as gd
        left outer join cte_source_games as g on gd.game_id = g.game_id
        where len(gd.power_up_type) > 0
        group by
            gd.booking_reference,gd.sk_site,
            gd.event_date,
            gd.game_id,
            g.game_name,
            gd.game_mode,
            gd.game_type,

            date_part(hour, gd.message_time),
            gd.box_id,
            gd.difficulty,
            gd.difficulty_name,
            upper(power_up_type)
    )

select *
from cte_main
