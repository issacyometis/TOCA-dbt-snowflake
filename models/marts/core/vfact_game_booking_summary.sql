with
    cte_source_fact_game_command as (select * from {{ ref("fact_game_command") }}),

    cte_source_fact_game_detail as (select * from {{ ref("fact_game_detail") }}),
    cte_players_per_game as (

        select
            booking_reference,
            unique_game_id,
            count(distinct player_id) as number_of_players
        from cte_source_fact_game_detail
        group by booking_reference, unique_game_id
    ),

    cte_players_per_booking as (
        select booking_reference, max(number_of_players) as number_of_players
        from cte_players_per_game
        group by booking_reference
    ),

    cte_game_activate as (

        select
            sk_site, 
            booking_reference,
            venue_id,
            box_id,
            booking_duration,
            team_name,
            min(message_timestamp) as booking_starttime
        from cte_source_fact_game_command
        where message_type = 'GAME_ACTIVATE'
        group by sk_site, booking_reference, venue_id, box_id, booking_duration, team_name
    ),

    cte_game_terminate as (

        select
            sk_site, 
            booking_reference,
            venue_id,
            box_id,
            booking_duration,
            team_name,
            max(message_timestamp) as booking_endtime
        from cte_source_fact_game_command
        where message_type = 'GAME_TERMINATE'
        group by sk_site, booking_reference, venue_id, box_id, booking_duration, team_name
    ),

    cte_game_extend as (

        select
            sk_site, 
            booking_reference,
            venue_id,
            box_id,
            sum(time_extension) as time_extension_minutes,
            count(*) as no_of_extensions
        from cte_source_fact_game_command
        where message_type = 'GAME_EXTEND_SESSION'
        group by sk_site, booking_reference, venue_id, box_id
    ),

    cte_game_pause as (

        select
            sk_site, 
            booking_reference,
            venue_id,
            box_id,
            count(*) as number_of_pauses,
            sum(
                case when reason = 'HOST_PAUSE' then 1 else 0 end
            ) as number_of_host_pauses,
            sum(
                case when reason = 'DEAD_BALLS' then 1 else 0 end
            ) as number_of_dead_balls,
            sum(
                case when reason = 'DO_NOT_CROSS' then 1 else 0 end
            ) as number_of_do_not_cross
        from cte_source_fact_game_command
        where message_type = 'GAME_PAUSE'
        group by sk_site, booking_reference, venue_id, box_id
    ),

    cte_games_details as (

        select
           sk_site,  booking_reference, venue_id, box_id, max(message_timestamp) as game_end_time
        from cte_source_fact_game_detail
        group by sk_site,  booking_reference, venue_id, box_id
    ),

    cte_stg as (

        select
            a.sk_site, 
            a.booking_reference,
            a.venue_id,
            a.box_id,
            a.booking_duration,
            -- A.Team_Name, 
            a.booking_starttime,
            t.booking_endtime,
            datediff(
                minute,
                a.booking_starttime,
                coalesce(t.booking_endtime, g.game_end_time)
            )
            / 60 as booking_hours,
            e.time_extension_minutes,
            e.no_of_extensions,
            p.number_of_pauses,
            p.number_of_host_pauses,
            p.number_of_dead_balls,
            p.number_of_do_not_cross,
            case
                when pb.number_of_players > 12 then 12 else pb.number_of_players
            end as number_of_players
        from cte_game_activate as a
        left join
            cte_game_terminate as t
            on a.booking_reference = t.booking_reference
            and a.box_id = t.box_id
            and a.venue_id = t.venue_id
        left join
            cte_game_extend as e
            on a.booking_reference = e.booking_reference
            and a.box_id = e.box_id
            and a.venue_id = e.venue_id
        left join
            cte_game_pause as p
            on a.booking_reference = p.booking_reference
            and a.box_id = p.box_id
            and a.venue_id = p.venue_id
        left join
            cte_games_details as g
            on a.booking_reference = g.booking_reference
            and a.box_id = g.box_id
            and a.venue_id = g.venue_id
        left join
            cte_players_per_booking as pb on a.booking_reference = pb.booking_reference
    ), 
cte_main as (
select booking_reference, sk_site 
, sum(time_extension_minutes)as time_extension_minutes
, sum (no_of_extensions)as no_of_extensions
, MAX(number_of_players) as number_of_players
, sum(number_of_host_pauses) as number_of_host_pauses
, sum(number_of_dead_balls) as number_of_dead_balls
, sum(number_of_do_not_cross) as number_of_do_not_cross
, sum(number_of_pauses) as number_of_pauses
 from cte_stg
 group by booking_reference, sk_site

)


select *
from cte_main
