{{ config(alias="fact_sentiment") }}

with
    sentiment as (
        {{
            dbt_utils.snowflake__deduplicate(
                ref("stg_sentiment__sentiment_tahola"),
                "site_name, file_date",
                "source_modified_at desc",
            )
        }}
    ),
    dim_business_drivers as (select * from {{ ref("dim_business_drivers") }}),
    dim_date as (select * from {{ ref("dim_date") }}),

    final as (
        select
            -- keys        
            dim_business_drivers.sk_site,

            -- ids
            sentiment.site_name,
            sentiment.review_count,
            sentiment.review_count_comparison,

            -- dates
            dim_date.trans_date as file_date, 

            -- times

            -- measures
            sentiment.rating,
            sentiment.rating_comparison,
            sentiment.competitor_rating,
            sentiment.competitor_rating_comparison,
            sentiment._5star,
            sentiment._4star,
            sentiment._3star,
            sentiment._2star,
            sentiment._1star,
            sentiment.nps,
            sentiment.nps_comparison,
            sentiment.critical,

            sentiment.food_average_sentiment,
            sentiment.food_mentions,
            sentiment.service_average_sentiment,
            sentiment.service_mentions,
            sentiment.ambience_average_sentiment,
            sentiment.ambience_mentions,
            sentiment.cleanliness_average_sentiment,
            sentiment.cleanliness_mentions

        from sentiment
        left join dim_business_drivers on dim_business_drivers.sentiment_site_name = sentiment.site_name 
        left join dim_date on dim_date.trans_date = sentiment.file_date

    )

select *
from final

