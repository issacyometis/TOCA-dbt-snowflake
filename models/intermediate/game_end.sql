with cte_source_Games as (

select * from {{ref('stg_gaming__game_status')}}
where Event_Type IN ('GAME_END')
) 


, cte_Game_End as (

Select 

game_status_id as ID,
Booking_reference as Booking_Reference, 
venue_id as venue_ID,
box_id as Box_ID,
Game_ID as GAME_ID,
--concat(Game_ID, Message_timestamp)as Unique_Game_ID,, 
LAG(MESSAGE_TIMESTAMP) OVER (PARTITION BY BOOKING_REFERENCE, box_id ORDER BY MESSAGE_TIMESTAMP) AS Prv_GAME_END_TimeStamp,
Message_timestamp as Game_End_TimeStamp,
ifnull(datediff(second, LAG(MESSAGE_TIMESTAMP) OVER (PARTITION BY BOOKING_REFERENCE, box_id ORDER BY MESSAGE_TIMESTAMP), Message_timestamp ),60) as seconds_between_end,
1 as Game_End_Message_Recieved

 from cte_source_Games

), main as (
select * from cte_Game_end
--where seconds_between_end>4

)

select * from cte_Game_end
--where booking_reference = '344J5F72D' --'EN1PPURN'