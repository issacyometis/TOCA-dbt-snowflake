with
    cte_source as (

        {{
            dbt_utils.snowflake__deduplicate(
                ref("stg_sevenrooms__charges"),
                "ID",
                "Insert_ID desc",
            )
        }}

    ),

    cte_main as (

        select
            additional_fee,
additional_fee_tax,
amount,
base_amount,
brand,
charged,
to_date(charged::timestamp_ltz) as charged_date,
to_time(charged::timestamp_ltz) as charged_time,
entity_date,
entity_id as Booking_id,
failure_code,
failure_message,
gratuity_amount,
id,
is_info_request,
is_refund,
last_4,
notes,
original_amount,
processing_fee,
promo_code,
promo_discount_amount,
service_charge_amount,
status,
tax_amount,
transaction_id,
transaction_type,
upsell_amount,
venue_group_client_id,
venue_id,
1 as payment_count
        from cte_source

    )

select *
from cte_main
