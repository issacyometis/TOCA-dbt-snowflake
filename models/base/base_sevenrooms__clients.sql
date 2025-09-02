with
    cte_source as (

        {{
            dbt_utils.snowflake__deduplicate(
                ref("stg_sevenrooms__clients"),
                "ID",
                "Insert_ID desc",
            )
        }}

    ),

    cte_main as (

        select
            ID as Client_id, 
            birthday_day, 
            birthday_month, 
            client_tags,
            company, 
            country, 
            created as Created_at, 
            custom_fields, 
            deleted as is_deleted, 
            email, 
            external_reference_code, 
            external_user_id, 
            first_name, 
            gender, 
            has_billing_profile as is_billing_profile, 
            is_contact_private, 
            is_one_time_guest, 
            last_name, 
            loyalty_id, 
            loyalty_tier, 
            loyalty_rank, 
            marketing_opt_in, 
            marketing_opt_in_ts as  marketing_opt_in_at, 
            marketing_opt_out_ts as marketing_opt_out_at, 
            notes,
            phone_number, 
            postal_code as Post_code,
            status,
            tags,
            title,
            total_cancellations,
            total_covers,
            total_noshows,
            total_order_count,
            total_spend,
            total_spend_per_cover,
            total_spend_per_visit,
            total_visits, 
            updated as Updated_at, 
                  Insert_ID, 
            Insert_date      
            
             from cte_source

    )

select *
from cte_main
