with cte_source_fact_game_summary as (

select * from {{ref('fact_game_summary')}}

)  , 

cte_source_game_players as (

    select * from {{ref('game_players')}}
) ,


cte_Top_Score as (

    select sk_site, Booking_Reference, Venue_ID, Box_ID, 
    MAX(CASE WHEN Game_ID = 'STRIKER'THEN  Score ELSE  0 end)  as Striker_Top_Score,
    MAX(CASE WHEN Game_ID = 'ATOMSPLITTER'THEN  Score ELSE  0 end)  as ATOMSPLITTER_Top_Score,
    MAX(CASE WHEN Game_ID = 'ELIMINATOR'THEN  Score ELSE  0 end)  as ELIMINATOR_Top_Score,
    MAX(CASE WHEN Game_ID = 'TOCACHALLENGE'THEN  Score ELSE  0 end)  as TOCACHALLENGE_Top_Score
     from cte_source_fact_game_summary
     group by sk_site, Booking_Reference, Venue_ID, Box_ID
)
,

cte_Game_Players_Summary as (

select
fgs.sk_site,  
fgs.Booking_reference as Booking_Reference, 
fgs.Venue_ID as Venue_ID,
fgs.Box_id as Box_ID,
fgs.Message_Date,
fgs.Player_ID, 
GP.Player_Name,
GP.Contact, 
GP.EncryptEmails,
gp.IS_GDPR_Accepted, 
gp.IS_GDPR_Underage, 
fgs.response_time,
CASE WHEN Game_ID = 'STRIKER'THEN  Score ELSE  0 end  as Striker_Score, 
CASE WHEN Game_ID = 'ATOMSPLITTER'THEN  Score ELSE  0 end  as ATOMSPLITTER_Score, 
CASE WHEN Game_ID = 'ELIMINATOR'THEN  Score ELSE  0 end  as ELIMINATOR_Score, 
CASE WHEN Game_ID = 'TOCACHALLENGE'THEN  Score ELSE  0 end  as TOCACHALLENGE_Score

 from cte_source_fact_game_summary as fgs
left outer join cte_source_game_players as gp on fgs.Booking_Reference = gp.Booking_Reference
and fgs.Box_ID = gp.Box_ID
and fgs.Player_ID = gp.Player_ID
AND fgs.MESSAGE_TIMESTAMP >= GP.Player_Start_TimeStamp
AND fgs.MESSAGE_TIMESTAMP < COALESCE(GP.PLAYER_END_TimeStamp, TO_TIMESTAMP('2099-12-31')) 

), 

cte_main as (

select
f.sk_site,   
f.Booking_Reference,
f.Venue_ID,
f.Box_ID,
f.Message_Date ,
f.Player_ID,
f.Player_name,
f.Contact,
f.EncryptEmails,
f.IS_GDPR_Accepted,
f.IS_GDPR_Underage,
SUM(f.response_time) as total_response_time,
MAX(f.Striker_Score) as Striker_Score,
MAX(f.ATOMSPLITTER_Score) as ATOMSPLITTER_Score,
MAX(f.ELIMINATOR_Score) as ELIMINATOR_Score,
max(f.TOCACHALLENGE_Score) as TOCACHALLENGE_Score,
MAX(T.Striker_Top_Score) as Striker_Top_Score,
MAX(T.ATOMSPLITTER_Top_Score) as ATOMSPLITTER_Top_Score,
MAX(T.ELIMINATOR_TOP_Score) as ELIMINATOR_Top_Score,
MAX(T.TOCACHALLENGE_TOP_Score) as TOCACHALLENGE_Top_Score

 from cte_Game_Players_Summary as f 
 left outer join cte_Top_Score as t on 
f.Booking_Reference = T.Booking_Reference
and f.Venue_ID = T.Venue_ID
and f.Box_ID = t.Box_ID

group by
f.sk_site, 
f.Booking_Reference,
f.Venue_ID,
f.Box_ID,
f.Message_Date ,
f.Player_ID,
f.Player_name,
f.Contact,
f.EncryptEmails,
f.IS_GDPR_Accepted,
f.IS_GDPR_Underage
)

select * from cte_main
