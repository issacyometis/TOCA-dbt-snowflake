with
    cte_stg_customer as (select * from {{ ref("base_sevenrooms__clients") }}),

    cte_main as (

        select
        Client_id as SK_Clients,
            email,
            title,
            first_name,
            last_name,
            gender,
            company,
            marketing_opt_in,
            notes,
            phone_number,
            created_at,
            is_deleted,
            is_one_time_guest,
            is_contact_private,
            total_cancellations,
            total_covers,
            total_noshows,
            total_order_count,
            total_spend,
            total_spend_per_cover,
            total_spend_per_visit,
            total_visits, 
            Post_code
        from cte_stg_customer

    )

select *
from cte_main
