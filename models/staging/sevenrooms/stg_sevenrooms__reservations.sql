with
    cte_source as (
        select * from {{ source("sevenrooms", "RESERVATIONS") }}
    ), 

    cte_main as ( 
   select 
   ID as Insert_ID, 
   Insert_date, 
replace(value:access_persistent_id,'"', '') as access_persistent_id ,
replace(value:address,'"', '') as address,
replace(value:address_2,'"', '') as address_2,
replace(value:arrival_time,'"', '') as arrival_time,
replace(value:arrived_guests,'"', '') as arrived_guests,
replace(value:booked_by,'"', '') as booked_by,
replace(value:check_numbers,'"', '') as check_numbers,
replace(value:city,'"', '') as city,
replace(value:client_id,'"', '') as client_id,
replace(value:client_reference_code,'"', '') as client_reference_code,
replace(value:client_requests,'"', '') as client_requests,
replace(value:comps,'"', '') as comps,
replace(value:comps_price_type,'"', '') as comps_price_type,
replace(value:cost_option,'"', '') as cost_option,
replace(value:country,'"', '') as country,
replace(value:created,'"', '') as created,
replace(value:custom_fields,'"', '') as custom_fields,
replace(value:date,'"', '') as date,
replace(value:deleted,'"', '') as deleted,
replace(value:duration,'"', '') as duration,
replace(value:email,'"', '') as email,
replace(value:external_client_id,'"', '') as external_client_id,
replace(value:external_id,'"', '') as external_id,
replace(value:external_reference_code,'"', '') as external_reference_code,
replace(value:external_user_id,'"', '') as external_user_id,
replace(value:first_name,'"', '') as first_name,
replace(value:id,'"', '') as id,
replace(value:is_vip,'"', '') as is_vip,
replace(value:last_name,'"', '') as last_name,
replace(value:left_time,'"', '') as left_time,
replace(value:loyalty_id,'"', '') as loyalty_id,
replace(value:loyalty_rank,'"', '') as loyalty_rank,
replace(value:loyalty_tier,'"', '') as loyalty_tier,
replace(value:max_guests,'"', '') as max_guests,
replace(value:mf_ratio_female,'"', '') as mf_ratio_female,
replace(value:mf_ratio_male,'"', '') as mf_ratio_male,
replace(value:min_price,'"', '') as min_price,
replace(value:modify_reservation_link,'"', '') as modify_reservation_link,
replace(value:notes,'"', '') as notes,
replace(value:onsite_payment,'"', '') as onsite_payment,
replace(value:onsite_payment_gratuity,'"', '') as onsite_payment_gratuity,
replace(value:onsite_payment_net,'"', '') as onsite_payment_net,
replace(value:onsite_payment_tax,'"', '') as onsite_payment_tax,
replace(value:onsite_payment_total,'"', '') as onsite_payment_total,
replace(value:paid_by,'"', '') as paid_by,
replace(value:phone_number,'"', '') as phone_number,
replace(value:pos_tickets,'"', '') as pos_tickets,
replace(value:postal_code,'"', '') as postal_code,
replace(value:prepayment,'"', '') as prepayment  ,
replace(value:prepayment_gratuity,'"', '') as prepayment_gratuity,
replace(value:prepayment_net,'"', '') as prepayment_net,
replace(value:prepayment_service_charge,'"', '') as prepayment_service_charge,
replace(value:prepayment_tax,'"', '') as prepayment_tax,
replace(value:prepayment_total,'"', '') as prepayment_total,
replace(value:rating,'"', '') as rating,
replace(value:real_datetime_of_slot,'"', '') as real_datetime_of_slot,
replace(value:reference_code,'"', '') as reference_code,
replace(value:reservation_sms_opt_in,'"', '') as reservation_sms_opt_in,
replace(value:reservation_type,'"', '') as reservation_type,
replace(value:seated_time,'"', '') as seated_time,
replace(value:send_reminder_email,'"', '') as send_reminder_email,
replace(value:send_reminder_sms,'"', '') as send_reminder_sms,
replace(value:served_by,'"', '') as served_by,
replace(value:shift_category,'"', '') as shift_category,
replace(value:shift_persistent_id,'"', '') as shift_persistent_id,
replace(value:source_client_id,'"', '') as source_client_id, 
replace(value:state,'"', '') as state, 
replace(value:status,'"', '') as status,
replace(value:status_display,'"', '') as status_display,
replace(value:status_simple,'"', '') as status_simple,
replace(value:table_numbers,'"', '') as table_numbers,
value:tags as tags,
replace(value:time_slot_iso,'"', '') as time_slot_iso,
replace(value:total_gross_payment,'"', '') as total_gross_payment,
replace(value:total_net_payment,'"', '') as total_net_payment,
replace(value:total_payment,'"', '') as total_payment,
replace(value:updated,'"', '') as updated,
value:upgrades as upgrades,
replace(value:user,'"', '') as user,
replace(value:using_default_duration,'"', '') as using_default_duration,
replace(value:venue_group_client_id,'"', '') as venue_group_client_id,
replace(value:venue_group_id,'"', '') as venue_group_id,
replace(value:venue_id,'"', '') as venue_id,
replace(value:venue_seating_area_id,'"', '') as venue_seating_area_id,
replace(value:venue_seating_area_name,'"', '') as venue_seat1ing_area_name
from cte_source 
,lateral flatten(input => JSON_DATA:data:results) d


  
     )

     


    select 
    
    * 
   from cte_main