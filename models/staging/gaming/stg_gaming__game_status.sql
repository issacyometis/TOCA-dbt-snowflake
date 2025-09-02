with 
    cte_source as (select * from {{ source('gaming', 'game_status') }} where SPLIT_PART(SPLIT_PART(__hevo_id, '/', 2), '-', 1) <> 'bir'), 

    cte_base as (

        select 
            -- ids
            _ID:"$oid"::string AS game_status_id,
            SPLIT_PART(SPLIT_PART(__hevo_id, '/', 2), '-', 1) as venue_id,
            SPLIT_PART(SPLIT_PART(__hevo_id, '/', 2), '-', 2) as box_id,
            SPLIT_PART(SPLIT_PART(__hevo_id, '/', 2), '-', 8) as booking_reference,
            GAMEID AS game_id,
            PLAYERID AS player_id,
            BALLID AS ball_id,
            
            --strings
            MESSAGETYPE AS message_type,
            MESSAGEORIGIN AS message_origin,
            EVENTTYPE AS event_type,
            NICKNAME AS nickname,
            CONTACT AS contact,
            GAMETYPE AS game_type,
            GAMEMODE AS game_mode,
            CLIPPATH AS clip_path,
            AVATARPATH AS avatar_path,
            ACTIONREPLAY AS action_replay_path,
            STATISTICS::array AS statistics_array,
            SLOTS::array AS slots_array,
            BENCHEDPLAYERS::array AS benched_players_array,
            Type as POWER_UP_TYPE,

            --numerics
            CURRENTROUND AS current_round,
            NUMROUNDS AS number_rounds,
            SHOTINDEX AS shot_index,
            SCORE AS score,
            RESPONSETIME AS response_time,
            TOCACHALLENGESTAGE AS toca_challenge_stage,
            TOCACHALLENGELEVEL AS toca_challenge_level,
            DIFFICULTY AS difficulty,
            SPEED AS speed,

            --booleans
            ISMISS AS is_miss,
            ISTIMEOUT AS is_timeout,
            CLIPSELECTED AS is_clip_selected,
            ISTOPBINS AS is_top_bins,
            ISGAMEWINNING AS is_game_winning,
            ISKNOCKOUT AS is_knockout,
            ISCHAINREACTION AS is_chain_reaction,
            ISSMALLESTTARGET AS is_smallest_target,
            GDPR_ACCEPTED AS is_gdpr_accepted,
            GDPR_UNDERAGE AS is_gdpr_underage,
            ISTEMPORARYID AS is_temporary_id,
            IFF(GAMEID = 'TOCACHALLENGE'
                AND SHOTINDEX = 14
                AND ISMISS = 0
                AND ISTIMEOUT = 0
                AND TOCACHALLENGESTAGE = 3
                AND TOCACHALLENGELEVEL = 5
            , TRUE, FALSE) AS is_toca_win,

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
    partition_by='game_status_id',
    order_by='event_timestamp desc',
   )
}}