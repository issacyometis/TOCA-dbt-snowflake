with
    cte_source as (

        {{
            dbt_utils.snowflake__deduplicate(
                ref("stg_sevenrooms__reservations"),
                "reference_code",
                "Insert_ID desc",
            )
        }}

    ),

    cte_main as (

        select
           
            insert_id as inserted_id,
            insert_date as inserted_at,
            Id as Sevenrooms_reservation_id, 
            arrival_time as fixture_time,
            arrived_guests,
            booked_by,
            client_id,
            cost_option,
            created::timestamp_ltz as booking_created_at,
            to_date(created::timestamp_ltz) as booking_created_date,
            to_time(created::timestamp_ltz) as booking_created_time,
            custom_fields,
            real_datetime_of_slot::timestamp_ltz as fixture_at,
            to_date(real_datetime_of_slot) as fixture_date,
            deleted::timestamp_ltz as deleted_at,
            to_date(deleted::timestamp_ltz) as booking_deleted_date,
            to_time(deleted::timestamp_ltz) as booking_deleted_time,
            iff(deleted is null, 0, 1) as is_deleted,
            email as booking_email_address,
            md5(email) as encrypted_booking_email_address,
            first_name,
            is_vip,
            last_name,
            left_time,
            max_guests,
            onsite_payment,
            onsite_payment_gratuity,
            onsite_payment_net,
            onsite_payment_tax,
            onsite_payment_total,
            paid_by,
            phone_number,
            postal_code as post_code,
            prepayment,
            prepayment_gratuity,
            prepayment_net,
            prepayment_service_charge,
            prepayment_tax,
            prepayment_total,
            rating,
            --real_datetime_of_slot as fixture_at,
            reference_code as reference,
            reservation_sms_opt_in as is_sms_opt_in,
            reservation_type,
            shift_category,
            status,
            status_display,
            status_simple,
            table_numbers,
            total_gross_payment,
            total_net_payment,
            total_payment,
            comps as discount_amount,
            updated::timestamp_ltz as booking_updated_at,
            to_date(updated::timestamp_ltz) as booking_updated_date,
            to_time(updated::timestamp_ltz) as booking_updated_time,
            duration as booking_duration,
            1 as booking_count, 
            notes,
            Venue_ID,
            upgrades,
            tags
        from cte_source

    )

select *
from cte_main
