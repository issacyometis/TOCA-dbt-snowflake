with
    cte_source_games as (select * from {{ ref("stg_gaming__game_status") }}),
    games as (select distinct game_id from cte_source_games),

    cte_main as (
        select
            *,
            case
                when game_id = 'STRIKER'
                then 'Striker'
                when game_id = 'ATOMSPLITTER'
                then 'Atom Splitter'
                when game_id = 'ELIMINATOR'
                then 'Eliminator'
                when game_id = 'TOCACHALLENGE'
                then 'Toca Challenge'
                when game_id = 'DEMOMODE'
                then 'Demo Mode'
                when game_id = 'ZOMBIES'
                then 'Zombies'
                else game_id
            end as game_name
        from games
        where game_id is not null 

    )
select *
from cte_main
