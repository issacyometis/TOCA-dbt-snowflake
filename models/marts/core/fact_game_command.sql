with cte_stg_game_command as (

select * from {{ref('stg_gaming__game_command')}}
) ,

cte_bussiness_drivers as (select * from {{ref('dim_business_drivers')}} )

, cte_fact_game_command as (

select 
b.sk_site,
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
Team_Name, 
Booking_Duration


from cte_stg_game_command as C
left outer join cte_bussiness_drivers as b on C.venue_id = b.GAMING_VENUE_ID

) 

select * from cte_fact_game_command