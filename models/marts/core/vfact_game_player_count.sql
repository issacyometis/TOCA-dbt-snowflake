with
    cte_source_gamesdetail as (select * from {{ ref("fact_game_detail") }}),
   
    cte_main as (

        select
            sk_site,
            booking_reference,
            count(distinct player_name) as Competing_players
            
             from cte_source_gamesdetail
             where Event_type = 'PLAYER_SHOT'
             and game_mode<>'DEMOMODE'

             group by sk_site, booking_reference

    )

select *
from cte_main
