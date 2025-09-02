with
    cte_source_gamesdetail as (select * from {{ ref("fact_game_detail") }}),
    cte_source_games as (select * from {{ ref("games") }}),

    Player_summary_level as (

        select
            sk_site, 
           booking_reference, 
           player_name,
           unique_game_id as game_start,
           max(shot_index) as Max_shot, 
           round(sum(RESPONSE_TIME),2) as Total_time 
        from cte_source_gamesdetail as gd
        where GAME_ID = 'TOCACHALLENGE'
        and IS_MISS = 0
        and IS_TIMEOUT = 0
        group by sk_site, BOOKING_REFERENCE, PLAYER_NAME, UNIQUE_GAME_ID
    ) , 

    Player_summary_stage as (

select 
sk_site, 
 booking_reference, 
           player_name,
           unique_game_id as game_start,
           shot_index as Max_shot, 
           Max(TOCA_CHALLENGE_STAGE) as Max_stage 

 from cte_source_gamesdetail
 where 
 GAME_ID = 'TOCACHALLENGE'
and IS_MISS = 0
and IS_TIMEOUT = 0
group by sk_site, BOOKING_REFERENCE, PLAYER_NAME, UNIQUE_GAME_ID, SHOT_INDEX
    ),


player_game_summary as (
select 
sk_site, 
 booking_reference, 
           player_name,
           unique_game_id as game_start,
           shot_index as Max_shot, 
           Max(TOCA_CHALLENGE_LEVEL) as max_level 

 from cte_source_gamesdetail
 where 
 GAME_ID = 'TOCACHALLENGE'
and IS_MISS = 0
and IS_TIMEOUT = 0
group by sk_site, BOOKING_REFERENCE, PLAYER_NAME, UNIQUE_GAME_ID, SHOT_INDEX

),

    Player_summary_max_shot as (

        select 
        l.sk_site, 
        l.booking_reference, 
        l.player_name,
        l.game_start,
        l.Max_shot,
        l.Total_time,
        s.Max_stage,
        g.max_level,
        row_number() over (order by l.Max_shot desc, l.Total_time asc) as Row_num
        
        from Player_summary_level as l
        left outer join Player_summary_stage as s on l.booking_reference = s.booking_reference
        and l.player_name = s.player_name
        and l.game_start =s.game_start
        and l.Max_shot =s.Max_shot
        left outer join player_game_summary as G on l.booking_reference = g.booking_reference
        and l.player_name = g.player_name
        and l.game_start =g.game_start
        and l.Max_shot =g.Max_shot
    )


select *
from Player_summary_max_shot
