with
    cte_source_games_status as (select * from {{ ref("game_status") }}),
    cte_source_game_players as (select * from {{ ref("game_players") }}),

    cte_source_game_start as (select * from {{ ref("game_start") }}),

    cte_source_game_rounds as (select * from {{ ref("game_rounds") }}),

    cte_source_game_score as (select * from {{ ref("game_score") }}),
    cte_bussiness_drivers as (select * from {{ref('dim_business_drivers')}} ),
    cte_stage as (

        select
            b.sk_site,
            gd.*,
            cast(gd.message_timestamp as date) as message_date,
            gp.player_name,
            gp.contact,
            gp.difficulty,
            gp.difficulty_name,
            is_gdpr_accepted,
            is_gdpr_underage,
            encryptemails as md5contact,
            gs.unique_game_id
        from cte_source_games_status as gd

        left outer join cte_bussiness_drivers as b on gd.venue_id = b.GAMING_VENUE_ID

        left outer join
            cte_source_game_players as gp
            on gd.booking_reference = gp.booking_reference
            and gd.player_id = gp.player_id
            and gd.message_timestamp >= gp.player_start_timestamp
            and gd.message_timestamp
            < coalesce(gp.player_end_timestamp, to_timestamp('2099-12-31'))

        left outer join
            cte_source_game_start as gs
            on gd.booking_reference = gs.booking_reference
            and gd.box_id = gs.box_id
            and gd.game_id = gs.game_id
            and gd.message_timestamp >= gs.game_start_timestamp
            and gd.message_timestamp
            < coalesce(gs.game_end_timestamp, to_timestamp('2099-12-31'))

        left outer join
            cte_source_game_rounds as gr
            on gd.booking_reference = gr.booking_reference
            and gd.box_id = gr.box_id
            and gd.game_id = gr.game_id
            and gd.message_timestamp >= gr.round_start_timestamp
            and gd.message_timestamp
            < coalesce(gr.round_end_timestamp, to_timestamp('2099-12-31'))

    ),

    cte_main as (

        select c.*, s.total_score
        from cte_stage as c
        left outer join
            cte_source_game_score as s
            on c.unique_game_id = s.unique_game_id
            and c.player_id = s.player_id

    )

select *
from cte_main
