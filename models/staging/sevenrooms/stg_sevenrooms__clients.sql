with
    cte_source as (
        select * from {{ source("sevenrooms", "CLIENTS") }}
    ), 

    cte_main as ( 
   select 
   ID as Insert_ID, 
   Insert_date, 
replace(value:address,'"', '') as address ,
replace(value:address_2,'"', '') as address_2,
replace(value:anniversary_day,'"', '') as anniversary_day,
replace(value:anniversary_month,'"', '') as anniversary_month,
replace(value:birthday_alt_day,'"', '') as birthday_alt_day,
replace(value:birthday_alt_month,'"', '') as birthday_alt_month,
replace(value:birthday_day,'"', '') as birthday_day,
replace(value:birthday_month,'"', '') as birthday_month,
replace(value:client_tags,'"', '') as client_tags,
replace(value:company,'"', '') as company,
replace(value:country,'"', '') as country,
replace(value:created,'"', '') as created,
replace(value:custom_fields,'"', '') as custom_fields,
replace(value:deleted,'"', '') as deleted,
replace(value:date,'"', '') as date,
replace(value:duration,'"', '') as duration,
replace(value:email,'"', '') as email,
replace(value:email_alt,'"', '') as email_alt,
replace(value:external_reference_code,'"', '') as external_reference_code,
replace(value:external_user_id,'"', '') as external_user_id,
replace(value:first_name,'"', '') as first_name,
replace(value:gender,'"', '') as gender,
replace(value:has_billing_profile,'"', '') as has_billing_profile,
replace(value:id,'"', '') as id,
replace(value:is_contact_private,'"', '') as is_contact_private,
replace(value:is_one_time_guest,'"', '') as is_one_time_guest,
replace(value:last_name,'"', '') as last_name,
replace(value:loyalty_id,'"', '') as loyalty_id,
replace(value:loyalty_rank,'"', '') as loyalty_rank,
replace(value:loyalty_tier,'"', '') as loyalty_tier,
replace(value:marketing_opt_in,'"', '') as marketing_opt_in,
replace(value:marketing_opt_in_ts,'"', '') as marketing_opt_in_ts,
replace(value:marketing_opt_out_ts,'"', '') as marketing_opt_out_ts,
replace(value:member_groups,'"', '') as member_groups,
replace(value:notes,'"', '') as notes,
replace(value:phone_number,'"', '') as phone_number,
replace(value:phone_number_alt,'"', '') as phone_number_alt,
replace(value:phone_number_alt_locale,'"', '') as phone_number_alt_locale,
replace(value:phone_number_locale,'"', '') as phone_number_locale,
replace(value:photo,'"', '') as photo,
replace(value:photo_crop_info,'"', '') as photo_crop_info,
replace(value:postal_code,'"', '') as postal_code,
replace(value:preferred_language_code,'"', '') as preferred_language_code,
replace(value:private_notes,'"', '') as private_notes  ,
replace(value:reference_code,'"', '') as reference_code,
replace(value:salutation,'"', '') as salutation,
replace(value:state,'"', '') as state,
replace(value:status,'"', '') as status,
replace(value:tags,'"', '') as tags,
replace(value:title,'"', '') as title,
replace(value:total_cancellations,'"', '') as total_cancellations,
replace(value:total_covers,'"', '') as total_covers,
replace(value:total_noshows,'"', '') as total_noshows,
replace(value:total_order_count,'"', '') as total_order_count,
replace(value:total_spend,'"', '') as total_spend,
replace(value:total_spend_per_cover,'"', '') as total_spend_per_cover,
replace(value:total_spend_per_visit,'"', '') as total_spend_per_visit,
replace(value:total_visits,'"', '') as total_visits,
replace(value:updated,'"', '') as updated,
replace(value:user,'"', '') as user_array,
replace(value:venue_group_id,'"', '') as venue_group_id, 
replace(value:venue_stats,'"', '') as venue_stats
from cte_source 
,lateral flatten(input => JSON_DATA:data:results) d


  
     )

     


    select 
    
    * 
   from cte_main