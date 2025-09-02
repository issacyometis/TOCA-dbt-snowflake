with cte_source_status as (

select * from {{ref('stg_gaming__game_status')}}
where EVENT_TYPE IN ('GAME_END') 
) ,
cte_Game_status as (


select 
S.BOOKING_REFERENCE,
        S.VENUE_ID,
        S.BOX_ID,
        S.GAME_ID,
        S.MESSAGE_TIMESTAMP,
        ST.VALUE:playerID::int AS PLAYER_ID,
        ST.VALUE:gameTime::float AS GAME_TIME,
        ST.VALUE:score::int AS SCORE,
        ST.VALUE:pointsPerBall::int AS POINTS_PER_BALL,
        ST.VALUE:responseTime::float AS RESPONSE_TIME,
        ST.VALUE:shotSpeed::float AS SHOT_SPEED,
        ST.VALUE:totalHits::int AS HITS,
        ST.VALUE:totalMisses::int AS MISSES,
        ST.VALUE:bronzeMedals::int AS BRONZE_MEDALS,
        ST.VALUE:silverMedals::int AS SILVER_MEDALS,
        ST.VALUE:goldMedals::int AS GOLD_MEDALS,
        ST.VALUE:medalPoints::int AS MEDAL_POINTS,
        ST.VALUE:partnerID::int AS PARTNER_ID
    
 from 
cte_source_status as s, 
LATERAL FLATTEN(input => S.statistics_array) AS ST
)

select * from cte_Game_status
