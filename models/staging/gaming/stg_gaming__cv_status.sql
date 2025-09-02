with 
    cte_source as (select * from {{ source('gaming', 'cv_status') }}
    where SPLIT_PART(SPLIT_PART(__hevo_id, '/', 2), '-', 1) <> 'bir')
    , 

    cte_base as (

        select 
            -- ids
            _ID:"$oid"::string AS cv_status_id,
            SPLIT_PART(SPLIT_PART(__hevo_id, '/', 2), '-', 1) as venue_id,
            SPLIT_PART(SPLIT_PART(__hevo_id, '/', 2), '-', 2) as box_id,
            SPLIT_PART(SPLIT_PART(__hevo_id, '/', 2), '-', 8) as booking_reference,
            BALLID::int AS ball_id,

            --strings
            MESSAGETYPE AS message_type,
            MESAGEORIGIN AS message_origin,

            --numerics
            --COORD[0]::numeric(10,5) AS ball_coordinate_x,
            --COORD[1]::numeric(10,5) AS ball_coordinate_y,
            --COORD[2]::numeric(10,5) AS ball_coordinate_z,
            COORD::array AS ball_coordinates,
            SPEED::numeric(10, 8) AS ball_speed,
            DIRECTION::array AS ball_direction,

            --booleans

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
    partition_by='cv_status_id',
    order_by='event_timestamp desc',
   )
}}




