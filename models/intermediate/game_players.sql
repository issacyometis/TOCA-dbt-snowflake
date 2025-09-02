with cte_source_players as (

select * from {{ref('stg_gaming__game_status')}}
where Event_Type IN ('PLAYER_ADDED')
) 


, cte_players as (

select game_status_id as ID,
Booking_reference as Booking_Reference, 
venue_id as venue_ID,
box_id as Box_ID,
Player_id as Player_ID,
coalesce (trim(Nickname), concat(booking_reference, '-', player_id)) as Player_Name, 
Message_timestamp as Player_Start_TimeStamp, 
LEAD(MESSAGE_TIMESTAMP) OVER (PARTITION BY BOOKING_REFERENCE, box_id, PLAYER_ID ORDER BY MESSAGE_TIMESTAMP) AS PLAYER_END_TimeStamp,
Difficulty as Difficulty,
CASE WHEN Difficulty = 0 then 'Beginner'
WHEN Difficulty = 1 then 'Intermediate'
when Difficulty = 2 then 'Advanced' end as Difficulty_Name,
Contact,
CASE when len(Contact)>7 then 'Yes' else 'No' end as Is_Email_Address,
IS_GDPR_Accepted,
IS_GDPR_UnderAge, 
Avatar_path,
md5(Contact) as EncryptEmails
 from cte_source_players
) 

select * from cte_players