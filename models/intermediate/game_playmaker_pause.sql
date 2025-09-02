with cte_stg_game_command as (

select * from {{ref('stg_gaming__game_command')}}
) , cte_pause_time  as (

select 

game_command_id as ID, 
booking_reference as Booking_Reference,
venue_id as Venue_ID,
box_id as Box_id,
message_type as Message_Type,
message_Origin,  
Message_timestamp as message_timestamp,
to_date(Message_timestamp) as Message_Date,
to_time(Message_timestamp) as Message_Time,
event_timestamp as Event_TimeStamp,
to_date(event_timestamp) as Event_Date,
to_time(event_timestamp) as Event_Time,
time_extension as Time_Extension,
reason as Reason, 
additional_information as Additional_Information, 
Is_Save_state as IS_Save_State, 
Is_Force_close, 
lead(MESSAGE_TIMESTAMP) OVER (PARTITION BY BOOKING_REFERENCE, box_id ORDER BY MESSAGE_TIMESTAMP) AS Pause_end_time

from cte_stg_game_command as C
where message_type in ('GAME_PAUSE', 'GAME_RESUME')
and reason ='HOST_PAUSE'


) , 
cte_main as(

select ID, booking_reference, venue_ID, box_id, message_type, message_Origin, message_timestamp as pause_start, Message_Date, Message_Time, Event_Date, Event_Time, Reason, Additional_Information, pause_end_time, datediff(minute, message_timestamp, pause_end_time) as paused_in_minutes from cte_pause_time
where message_type = 'GAME_PAUSE'
)

select * from cte_main
