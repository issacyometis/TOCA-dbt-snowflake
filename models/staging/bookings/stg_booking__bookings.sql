with 
    --cte_source as (select * from {{ source('Booking', 'TOCA_BOOKINGS_BOOKING') }}),
cte_source as (
   {{
            dbt_utils.snowflake__deduplicate(
                source('Booking', 'TOCA_BOOKINGS_BOOKING'),
                "reference",
                "ID desc",
            )
        }}

),

    cte_main as (

        select 
            ID as Booking_ID,
            to_date("START") as Booking_date, 
            to_time("START") as Booking_time,
            ("START") as Booking_at,
            timediff(minutes, "START", "END") as Booking_Duration_Mins,
            floor(timediff(minutes, "START", "END")/30)*30 as Booking_Duration,
            case 
                when (dayofweek(to_date("START")) in (5,6)) then 'peak' -- Friday & Saturday
                when (to_time("START") between '16:00:00' and '23:59:00') then 'peak'
                else 'off-Peak'
            end as Price_Type,
            to_time(box_slot_start) as Box_start_time,
            to_time(box_slot_end) as Box_end_time, 
            created_at as Booking_created_at,
            to_date(created_at) as Booking_created_date,
            to_time(created_at) as Booking_created_time,
            updated_at as Booking_updated_at,
            to_date(updated_at) as Booking_updated_date,
            to_time(updated_at) as Booking_updated_time,
            deleted_at as Booking_deleted_at,
            to_date(deleted_at) as Booking_deleted_date,
            to_time(deleted_at) as Booking_deleted_time,    
            to_date(checkin_at) as Booking_checkin_date,
            to_time(checkin_at) as Booking_checkin_time,  
            case 
            when source = 'Online' then source
            when source = 'Walkin' then 'Kiosk'
            when source = 'Reception' and Type = 'Event' then 'Event'
            when source = 'Reception' and Type = 'Social' then 'Reception'
            when source = 'Reception' and Type = 'Other' then 'Reception'
            else source
            end          as Booking_Source, 
            Type as Booking_Type, 
            Venue_ID,
            reference as Booking_reference,
            session_id,
            price,
            price/100 as Booking_Price, 
            Guests_No,
            Notes, 
            Extras, 
            Status as Booking_status,
            iff(Status = 'Paid' and Booking_Source <> 'Reception', 1, 0) as Is_Paid_Online,
            -- is 'reception' only right, what about walkin??
            iff(Booking_deleted_date is not NULL, 1, 0) as Is_Deleted
   
        from 
            cte_source s 
            where __hevo__marked_deleted ='False'
    )

    select * from cte_main
   