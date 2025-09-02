with cte_source_game as (

select * from {{ref('fact_game_detail')}}
where Event_Type IN ('PLAYER_SHOT')
), cte_main as ( 

select BOOKING_REFERENCE, Box_ID, Unique_Game_ID, Player_ID, sum(SCORE) as Total_Score from cte_source_game

group by BOOKING_REFERENCE, Box_ID, Unique_Game_ID, Player_ID
) 

select * from cte_main
