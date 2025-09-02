with 
    cte_source as (select * from {{ source('epos', 'product_api') }}),

    cte_main as (

        select 
*

        
        
        
         from cte_source
    )

    select * from cte_main