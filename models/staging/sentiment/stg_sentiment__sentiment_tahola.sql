with
    src_sentiment as (select * from {{ source("sentiment", "SENTIMENT_TAHOLA") }}),

    final as (

        select
            label::varchar as site_name,
            reviewcount::integer as review_count,
            nullif(rtrim(reviewcountcomparison, '%'), '-')::numeric(38, 5)
                    / 100 as review_count_comparison,
            nullif(trim(rating), '-')::numeric(38, 5) as rating,
            nullif(trim(ratingcomparison, '%'), '-')::numeric(38, 5)
                    / 100 as rating_comparison,
            nullif(trim(competitorrating), '-')::numeric(38, 5) as competitor_rating,
            nullif(trim(competitorratingcomparison, '%'), '-')::numeric(38, 5)
                / 100 as competitor_rating_comparison,
            _5star::integer as _5star,
            _4star::integer as _4star,
            _3star::integer as _3star,
            _2star::integer as _2star,
            _1star::integer as _1star,
            nullif(trim(nps), '-')::integer as nps,
            nullif(trim(npscomparison), '-')::integer as nps_comparison,
            critical::integer as critical,

            nullif(trim(foodaveragesentiment), '-')::integer as food_average_sentiment,
            nullif(trim(foodmentions), '-')::integer as food_mentions,
            nullif(trim(serviceaveragesentiment), '-')::integer as service_average_sentiment,
            nullif(trim(servicementions), '-')::integer as service_mentions,
            nullif(trim(ambienceaveragesentiment), '-')::integer as ambience_average_sentiment,
            nullif(trim(ambiencementions), '-')::integer as ambience_mentions,
            nullif(trim(cleanlinessaveragesentiment), '-')::integer as cleanliness_average_sentiment,
            nullif(trim(cleanlinessmentions), '-')::integer as cleanliness_mentions,
            try_to_date(
                replace(
                    reverse(left(reverse(split_part(__hevo_id::varchar, '.', 1)), 10)),
                    '_',
                    '-'
                ),
                'YYYY-MM-DD'
            ) as file_date,

            to_timestamp(__hevo__source_modified_at::number / 1000) as source_modified_at,
            to_timestamp(__hevo__ingested_at::number / 1000) as ingested_at,
            to_timestamp(__hevo__loaded_at::number / 1000) as load_at

        from src_sentiment

    )

select *
from final
