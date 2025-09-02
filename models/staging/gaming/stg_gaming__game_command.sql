with 
    cte_source as (select * from {{ source('gaming', 'game_command') }} where SPLIT_PART(SPLIT_PART(__hevo_id, '/', 2), '-', 1) <> 'bir'), 

    cte_base as (

        select 
            -- ids
            _ID:"$oid"::string AS game_command_id,
            SPLIT_PART(SPLIT_PART(__hevo_id, '/', 2), '-', 1) as venue_id,
            SPLIT_PART(SPLIT_PART(__hevo_id, '/', 2), '-', 2) as box_id,
            SPLIT_PART(SPLIT_PART(__hevo_id, '/', 2), '-', 8) as booking_reference,
            
            --strings
            MESSAGETYPE AS message_type,
            MESSAGEORIGIN AS message_origin,
            REASON AS reason,
            ADDITIONALINFORMATION AS additional_information,
            --BOOKINGDATA:"booking_ref"::string AS booking_ref, --duplicate and less complete than above version
            BOOKINGDATA:"team_name"::string AS team_name,
            BOOKINGDATA:"players"::array AS players_array,

            --numerics
            BOOKINGDATA:"duration"::numeric AS booking_duration,
            TIMEEXTENSION AS time_extension,

            --booleans
            SAVESTATE AS is_save_state,
            FORCECLOSE AS is_force_close,

            --dates

            --timestamps
            timestampltzfromparts(
                SPLIT_PART(SPLIT_PART(__hevo_id, '/', 2), '-', 3),
                SPLIT_PART(SPLIT_PART(__hevo_id, '/', 2), '-', 4),
                SPLIT_PART(SPLIT_PART(SPLIT_PART(__hevo_id, '/', 2), '-', 5), 'T', 1),
                SPLIT_PART(SPLIT_PART(SPLIT_PART(__hevo_id, '/', 2), '-', 5), 'T', 2),
                SPLIT_PART(SPLIT_PART(__hevo_id, '/', 2), '-', 6),
                REPLACE(SPLIT_PART(SPLIT_PART(__hevo_id, '/', 2), '-', 7), 'Z', '')
                    ) AS event_timestamp,
            to_timestamp_ltz(MESSAGETIMESTAMP, 'YYYY-MM-DD HH24:MI:SS:FF') AS message_timestamp,
            to_timestamp_ltz(__HEVO__INGESTED_AT, 3) AS ingested_at_timestamp,
            to_timestamp_ltz(__HEVO__LOADED_AT, 3) AS loaded_at_timestamp,
            to_timestamp_ltz(__HEVO__SOURCE_MODIFIED_AT, 3) AS source_modified_at_timestamp

        from cte_source
    )

{{ dbt_utils.deduplicate(
    relation='cte_base',
    partition_by='game_command_id',
    order_by='event_timestamp desc',
   )
}}