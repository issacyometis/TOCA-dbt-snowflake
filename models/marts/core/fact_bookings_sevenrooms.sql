with
    cte_stg_bookings as (select * from {{ ref("base_sevenrooms__reservations") }}),
    cte_stg_payments as (select * from {{ ref("fact_sevenrooms_payments") }}),
    cte_site as (select * from {{ ref("dim_business_drivers") }}),
    cte_stg_upgrade as (select * from {{ref("fact_sevenrooms_upgrades")}}),
    cte_tbl_tags as (select * from {{ref("tbl_sevenrooms_tags")}}), 
    cte_source_fact_game_command as (select * from {{ ref("fact_game_command") }}),
    cte_boxes as (select * from {{ref("sevenrooms_boxes")}}),

cte_upgrade as (
select reference,  sum(Total_upgrade) as Total_upgrade, true as is_package_bookings from cte_stg_upgrade
group by reference

),

cte_box_count as (select reference, sum(is_box) as no_of_boxes from cte_boxes 
group by reference),


cte_tag_summary as (
select reference, 
max(iff(lower(group_name)='box booking', 1, 0)) as is_box_bookings, 
max(iff(lower(group_name)='packages', 1, 0) )as is_package_bookings, 
max(iff(lower(group_name)='private event', 1, 0)) as is_private_event_bookings, 
max(iff(lower(group_name)='special occasions', 1, 0)) as is_special_occasion_bookings, 
max(iff(lower(group_name)='table_bookings', 1, 0)) as is_table_bookings,
max(iff(lower(tag_display) = 'upgrade - 30 mins extra', 1, 0)) as is_sevenroom_extension
  from cte_tbl_tags
  group by reference


), 

cte_Payment_summary as (

select reference, 
sum(iff(is_refund = 'true', amount_pounds *-1, amount_pounds)) as Total_Amount, 
sum(iff(is_refund = 'true', amount_pounds *-1, 0)) as Refund_Amount, 
sum(iff(is_refund = 'true', 0, amount_pounds)) as Paid_Amount,
sum(payment_count) as Number_of_Payments,
max(iff(is_refund = 'true', CHARGED_DATE, null)) as Refund_date


from cte_stg_payments
GRoup by reference
), 


cte_game_extend as (

        select
            sk_site, 
            booking_reference as reference,
            sum(time_extension) as time_extension_minutes,
            count(*) as no_of_extensions
        from cte_source_fact_game_command
        where message_type = 'GAME_EXTEND_SESSION'
        group by sk_site, booking_reference
    ),

cte_exclude_bookings as (
select distinct reference, 1::boolean as is_exclude_box_time from cte_tbl_tags
where tag in('EUROS 2024', 'EXCLUDE')

),

    cte_main as (

        select
            s.sk_site,
            b.client_id as sk_clients,
            b.sevenrooms_reservation_id, 
            b.reference,
            b.booked_by, 
            b.booking_email_address, 
            b.encrypted_booking_email_address, 
            b.first_name, 
            b.last_name, 
            b.paid_by, 
            b.phone_number, 
            b.post_code, 
            b.reservation_type, 
            b.shift_category, 
            b.status, 
            b.status_display, 
            b.status_simple, 
            b.table_numbers, 
            b.notes,
            replace(substr(REGEXP_SUBSTR(b.notes, 'Ref\\W+\\w+'),5, 10),'| ', '') as Legacy_booking_reference, 
            
            b.arrived_guests::numeric as arrived_guests,
            b.max_guests::numeric as max_guests, 
            b.left_time as left_time, 
            b.onsite_payment::numeric(16,4) as onsite_payment, 
            b.onsite_payment_gratuity::numeric(16,4) as onsite_payment_gratuity, 
            b.onsite_payment_net::numeric(16,4) as onsite_payment_net, 
            b.onsite_payment_tax::numeric(16,4) as onsite_payment_tax, 
            b.onsite_payment_total::numeric(16,4) as onsite_payment_total, 
            b.prepayment::numeric(16,4) as prepayment, 
            b.prepayment_gratuity::numeric(16,4) as prepayment_gratuity, 
            b.prepayment_net::numeric(16,4) as prepayment_net, 
            b.prepayment_service_charge::numeric(16,4) as prepayment_service_charge, 
            b.prepayment_total::numeric(16,4) as prepayment_total, 
            b.rating::numeric(16,4) as rating, 
            b.total_gross_payment::numeric(16,4) as total_gross_payment, 
            b.total_net_payment::numeric(16,4) as total_net_payment,
            b.total_net_payment::numeric(16,4) /1.20 as total_net_payment_excluding_vat, 
            b.total_payment::numeric(16,4) as total_payment, 
            b.discount_amount::numeric(16,4) as discount_amount,
            b.booking_duration::numeric(16,4) as booking_duration ,  
            b.booking_count::numeric as booking_count, 
            iff(dayofweek(b.fixture_date) in (6, 7) or b.fixture_time > '16:00:00', 1, 0) as peak_booking_count,
            p.refund_amount::numeric(16,4) as refund_amount,
            u.total_upgrade::numeric(16,4) as total_upgrade, 
            ge.time_extension_minutes::numeric(16,4) as time_extension_minutes,
            ge.no_of_extensions::numeric as no_of_extensions, 
            try_TO_NUMBER(SPLIT_PART(SPLIT_PART(notes, 'Adults: ', 2), ',', 1)) AS no_of_adults,
            try_TO_NUMBER(SPLIT_PART(SPLIT_PART(notes, 'Kids: ', 2), ';', 1)) AS no_of_kids,
            bx.no_of_boxes,

            b.fixture_time, 
            b.booking_created_at, 
            b.booking_created_date,
            b.fixture_at, 
            b.fixture_date, 
            b.deleted_at, 
            b.booking_deleted_date, 
            b.booking_deleted_time,
            b.booking_updated_at, 
            b.booking_updated_date, 
            b.booking_updated_time, 
            p.refund_date,   
           
            
            b.cost_option::boolean as is_cost_option, 
            b.is_deleted, 
            b.is_vip, 
            b.is_sms_opt_in,
            iff(total_amount=0, 1, 0)::boolean as is_fully_refunded,
            ifnull(t.is_box_bookings,false)::boolean as is_box_bookings, 
            ifnull(u.is_package_bookings,false)::boolean as is_package_bookings, 
            ifnull(t.is_private_event_bookings,false)::boolean as is_private_event_bookings, 
            ifnull(t.is_special_occasion_bookings,false)::boolean as is_special_occasion_bookings, 
            ifnull(t.is_table_bookings,false)::boolean as is_table_bookings, 
            ifnull(e.is_exclude_box_time,false)::boolean as is_exclude_box_time,
            ifnull(t.is_sevenroom_extension,false)::boolean  as is_sevenroom_extension

        from cte_stg_bookings as b 
        left outer join cte_Payment_summary as P on p.reference = b.reference
        left outer join cte_site as s on b.Venue_ID = s.SEVENROOMS_ID
        left outer join cte_upgrade as u on b.reference = u.reference
        left outer join cte_tag_summary as t on b.reference = t.reference
        left outer join cte_exclude_bookings as e on b.reference = e.reference
        left outer join cte_game_extend as ge on b.reference = ge.reference and s.sk_site = ge.sk_site
        left outer join cte_box_count as bx on b.reference = bx.reference
       
    )

select *
from cte_main




