with
    fact_Sentiment as (select * from {{ ref("fact_sentiment") }}),
    dim_date as ( select * from {{ ref("dim_date") }} ),     

    sentiment_summary as (

        select
            -- keys        
            sk_site ,

            -- dates
            dim_date.trans_date as  file_date,
        
            -- dimensions            
            _5star   ,
            _4star   ,
            _3star   ,
            _2star   ,
            _1star   

        from fact_Sentiment
            join dim_date on dim_date.trans_date = fact_Sentiment.file_date
        where dim_date.relative_year in (0,1)               
    ), 

    final as (

        select * from sentiment_summary
        unpivot (star_count for stars in ( _5star, _4star, _3star, _2star, _1star ))

    )

select *
from     final 

