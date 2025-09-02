with
    cte_bookings_source as (select * from {{ ref("fact_booking") }}),
    cte_dim_booking_source as (select * from {{ ref("dim_booking_source") }}),
    cte_dim_booking_type as (select * from {{ ref("dim_booking_type") }}),
    cte_dim_booking_status as (select * from {{ ref("dim_booking_status") }}),
    cte_sevenroom_source as (select * from {{ ref("fact_sevenrooms") }}),
    cte_main as (

        select
            md5(1) as sk_site,
            md5(b.e_mail) as sk_customer,
            b.booking_reference as reference,
            bs.booking_source as booking_source,
            bt.booking_type,
            bst.booking_status,
            b.booking_date as fixture_date,
            b.booking_time as fixture_time,
            b.booking_at as fixture_at,
            left(b.booking_time, 2) as fixture_start_hour,
            b.box_end_time as fixture_end,
            b.booking_created_date as booking_date,
            b.booking_created_at,
            left(b.booking_created_time, 2) as booking_created_hour,
            b.booking_updated_date as updated_date,
            b.booking_updated_at,
            left(b.booking_updated_time, 2) as booking_updated_hour,
            b.booking_deleted_date as deleted_date,
            b.booking_deleted_at,
            left(booking_deleted_time, 2) as booking_deleted_time,
            b.is_deleted,
            datediff(day, booking_created_date, b.booking_date) as lead_time_days,
            b.marketing_gdpr,
            b.marketing_newsletters,
            b.marketing_shared_pics_videos,
            b.is_fully_refunded,
            b.duration60mins_count,
            b.peak_booking_count,
            b.booking_duration,
            b.bookings_count,
            b.guests_no,
            b.post_code as post_code,
            b.actual_total as actual_sales_inc_vat,
            b.actual_subtotal as actual_sales_exc_vat,
            b.discount_incl_vat as actual_discount_inc_vat,
            iff(
                b.is_deleted = 0
                and b.is_fully_refunded = 0
                and bst.booking_status = 'Paid',
                b.actual_total,
                0
            ) as reporting_sales_inc_vat,
            iff(
                b.is_deleted = 0
                and b.is_fully_refunded = 0
                and bst.booking_status = 'Paid',
                b.actual_subtotal,
                0
            ) as reporting_actual_sales_exc_vat,
            iff(
                b.is_deleted = 0
                and b.is_fully_refunded = 0
                and bst.booking_status = 'Paid',
                b.discount_incl_vat,
                0
            ) as reporting_discount_incl_vat,
            b.refund_amount,
            b.e_mail,
            b.refund_date,
            'TOCA Bookings' as data_source,
            null as notes,
            null as legacy_booking_reference,
            0 as Total_upgrade
        from cte_bookings_source as b
        left outer join
            cte_dim_booking_source as bs on b.booking_source_key = bs.booking_source_key
        left outer join
            cte_dim_booking_type as bt on b.booking_type_key = bt.booking_type_key
        left outer join
            cte_dim_booking_status as bst
            on b.booking_status_key = bst.booking_status_key
        where booking_date < {{ var("switch_date") }}
        union
        select
            sk_site,
            md5(booking_email_address) as sk_customer,
            reference,
            case
                when booked_by = 'Booking Widget'
                then 'Online'
                when booked_by = 'TripleSeat Integration'
                then 'Event'
                when booked_by = 'Walk In'
                then 'Kiosk'  -- Need to confrm this
                when booked_by like '%Package%'
                then 'Social'
                else 'Reception'
            end as booking_source,

            case
                when booked_by = 'TripleSeat Integration'
                then 'event'
                when booked_by like '%Package%'
                then 'social'
                else 'other'
            end as booking_type,
            iff(
                status_display = 'Canceled', 'Cancelled', status_display
            ) as booking_status,
            fixture_date,
            fixture_time,
            fixture_at,
            left(fixture_time, 2) as fixture_start_hour,
            timestampadd(min, booking_duration, to_time(fixture_time)) as fixture_end,
            booking_created_date as booking_date,
            booking_created_at,
            left(booking_created_time, 2) as booking_created_hour,
            booking_updated_date as updated_date,
            booking_updated_at,
            left(booking_updated_time, 2) as booking_updated_hour,
            booking_deleted_date as deleted_date,
            deleted_at as booking_deleted_at,
            left(booking_deleted_time, 2) as booking_deleted_at,
            is_deleted,
            datediff(day, booking_created_date, fixture_date) as lead_time_days,
            null as marketing_gdpr,
            null as marketing_newsletters,
            null as marketing_shared_pics_videos,
            is_fully_refunded as is_fully_refunded,
            iff(booking_duration = 60, 1, 0) as duration60mins_count,
            peak_booking_count as peak_booking_count,
            booking_duration,
            booking_count,
            max_guests as guests_no,
            post_code,
            total_gross_payment as actual_sales_inc_vat,
            total_net_payment_excluding_vat as actual_sales_exc_vat,
            discount_amount as actual_discount_inc_vat,
            iff(
                status_display in ('Canceled', 'Payment Outstanding'),
                0,
                total_gross_payment
            ) as reporting_sales_inc_vat,
            iff(
                status_display in ('Canceled', 'Payment Outstanding'),
                0,
                total_net_payment_excluding_vat
            ) as reporting_sales_exc_vat,
            iff(
                status_display in ('Canceled', 'Payment Outstanding'),
                0,
                discount_amount
            ) as reporting_discount_inc_vat,
            iff(refund_amount = 0, null, refund_amount * -1) as refund_amount,
            booking_email_address as e_mail,
            refund_date,
            'Seven Rooms' as data_source,
            notes,
            legacy_booking_reference,
            Total_upgrade
        from cte_sevenroom_source
        where fixture_date >={{ var("switch_date") }}
    )

select *
from cte_main
