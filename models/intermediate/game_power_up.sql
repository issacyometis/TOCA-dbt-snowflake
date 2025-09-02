with cte_source_game_power_up as (

select * from {{ref('stg_gaming__game_status')}}
where Event_Type IN ('POWER_UP_HIT')
) 


,cte_game_Power_up as (

Select 

game_status_id as ID,
Booking_reference as Booking_Reference, 
venue_id as venue_ID,
box_id as Box_ID,
Game_ID as GAME_ID,
Player_ID as Player_ID, 
1 as Power_up_hit,
Power_up_type,
Current_Round as Current_Round,
number_rounds as Number_Rounds, 
Message_timestamp as Round_Start_TimeStamp,
LEAD(MESSAGE_TIMESTAMP) OVER (PARTITION BY BOOKING_REFERENCE, box_id ORDER BY MESSAGE_TIMESTAMP) AS Round_End_TimeStamp
from cte_source_game_power_up

)


select * from cte_game_Power_up