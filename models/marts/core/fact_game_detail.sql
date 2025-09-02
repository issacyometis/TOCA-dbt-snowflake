with cte_source_GamesDetail as (

select * from {{ref('stg_gaming__game_status')}}

),
cte_source_game_players as (

    select * from {{ref('game_players')}}
) ,

cte_source_game_rounds as (

    select * from {{ref('game_rounds')}}
) ,

cte_source_game_start as (

    select * from {{ref('game_start')}}
) ,




cte_bussiness_drivers as (select * from {{ref('dim_business_drivers')}} ),


cte_main as (

select 
b.sk_site,
GD.Game_Status_ID as Game_Status_ID ,
GD.BOOKING_REFERENCE as Booking_Reference,
GD.VENUE_ID as Venue_ID,
GD.BOX_ID as BOX_ID,
GD.MESSAGE_TIMESTAMP as Message_TimeStamp,
TO_DATE(GD.MESSAGE_TimeStamp) as Message_Date,
TO_TIME(GD.MESSAGE_TIMEStamp) as Message_Time,
GD.EVENT_TIMESTAMP,
TO_DATE(GD.EVENT_TIMESTAMP) as Event_Date,
TO_TIME(GD.EVENT_TIMESTAMP) as Event_Time,
GD.EVENT_TYPE,
GD.GAME_ID as Game_ID,
GS.Unique_Game_ID as Unique_Game_ID, 
GS.TOURNAMENT_ID, 
GS.GAME_TYPE as GAME_TYPE,
GS.GAME_MODE as GAME_MODE,
GS.Game_Start_TimeStamp,
GS.GAME_END_TimeStamp,
GR.Current_Round,
GR.Number_Rounds,
GP.PLAYER_ID,
GP.Player_Name,
GP.Difficulty,
GP.Difficulty_Name,
GP.Contact,
GP.EncryptEmails,
GP.IS_GDPR_Accepted,
GP.IS_GDPR_UnderAge, 
GD.BALL_ID,
GD.SHOT_INDEX,
GD.SCORE,
GD.IS_MISS,
GD.IS_TIMEOUT,
GD.IS_GAME_WINNING,
GD.IS_KNOCKOUT,
GD.IS_CHAIN_REACTION,
GD.IS_SMALLEST_TARGET,
GD.IS_TOP_BINS,
GD.IS_TOCA_WIN,
GD.TOCA_CHALLENGE_STAGE,
GD.TOCA_CHALLENGE_LEVEL,
GD.RESPONSE_TIME,
GD.SPEED, 
TIMESTAMPDIFF(Second, GS.Game_Start_TimeStamp,GS.GAME_END_TimeStamp) as TotalGameTime,
CASE WHEN Event_type = 'POWER_UP_DISPLAYED' then 1 else 0 end as POWER_UP_DISPLAYED,
CASE WHEN Event_type = 'POWER_UP_HIT' then 1 else 0 end as POWER_UP_HIT,
GD.Power_up_Type as Power_up_Type



 from cte_source_GamesDetail as GD

 LEFT OUTER JOIN cte_source_game_players as GP on 
 GD.BOOKING_REFERENCE = GP.BOOKING_REFERENCE
    AND GD.PLAYER_ID = GP.PLAYER_ID
    AND GD.MESSAGE_TIMESTAMP >= GP.Player_Start_TimeStamp
    AND GD.MESSAGE_TIMESTAMP < COALESCE(GP.PLAYER_END_TimeStamp, TO_TIMESTAMP('2099-12-31'))
    AND GD.BOX_ID = GP.Box_ID
    
  LEFT OUTER JOIN cte_source_game_start as GS on 
 GD.BOOKING_REFERENCE = GS.BOOKING_REFERENCE
    AND GD.Box_ID = GS.BOX_ID
    and GD.GAME_ID = GS.GAME_ID
    AND GD.MESSAGE_TIMESTAMP >= GS.GAME_Start_TimeStamp
    AND GD.MESSAGE_TIMESTAMP < COALESCE(GS.GAME_END_TimeStamp, TO_TIMESTAMP('2099-12-31'))
    

LEFT OUTER JOIN cte_source_game_rounds as GR on 
 GD.BOOKING_REFERENCE = GR.BOOKING_REFERENCE
    AND GD.Box_ID = GR.BOX_ID
    and GD.GAME_ID = GR.GAME_ID
    AND GD.MESSAGE_TIMESTAMP >= GR.Round_Start_TimeStamp
    AND GD.MESSAGE_TIMESTAMP < COALESCE(GR.Round_END_TimeStamp, TO_TIMESTAMP('2099-12-31'))

left outer join cte_bussiness_drivers as b on gd.venue_id = b.GAMING_VENUE_ID

)


select * from cte_main

