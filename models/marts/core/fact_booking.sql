with
    cte_stg_bookings as (select * from {{ ref("stg_booking__bookings") }}),

    cte_payment as (select * from {{ ref("stg_booking__payment") }}),

    cte_stg_customer as (select * from {{ ref("stg_booking__booking_customer") }}),

    cte_stg_booking_source as (select * from {{ ref("stg_input__booking_source") }}),

    cte_main as (

        select
            -- Keys
            md5(ifnull(bs.booking_source, b.booking_source)) as booking_source_key,
            md5(b.booking_type) as booking_type_key,
            md5(b.booking_status) as booking_status_key,
            SessionID as customer_key,
            md5(
                concat(dayname(b.booking_date), date_part(hour, b.booking_time))
            ) as day_time_period_key,
            b.venue_id as location_key,
            -- Dates
            b.booking_date,
            b.booking_created_date,
            b.booking_updated_date,
            b.booking_deleted_date,
            b.booking_checkin_date,
            -- Times
            b.booking_time,
            b.booking_created_time,
            b.booking_updated_time,
            b.booking_deleted_time,
            b.booking_checkin_time,
            -- DateTime
            b.booking_at,
            b.booking_created_at,
            b.booking_updated_at,
            b.booking_deleted_at,
            -- Non Dimensional Attributes
            b.booking_id,
            b.booking_reference,
            b.notes,
            b.extras,
            b.is_deleted,
            c.marketing_gdpr,
            c.marketing_newsletters,
            c.marketing_shared_pics_videos,
            coalesce(c.has_under_18, 'false') as has_under_18_in_booking,
            b.session_id as session_id,

            iff(
                is_paid_online = 1 and is_fully_refunded = 0, 1, 0
            ) as is_paid_online_booking,
            ifnull(p.is_fully_refunded, 0) as is_fully_refunded,

            -- Measures
            datediff(year, try_to_date(c.dob), b.booking_date) as age_at_booking,
            b.price,
            b.booking_price,
            b.guests_no,
            p.total as actual_total,
            iff(
                is_paid_online = 1 and is_fully_refunded = 0, ifnull(p.total, 0), 0
            ) as total_inc_vat,
            p.subtotal_excl_vat as actual_subtotal,
            iff(
                is_paid_online = 1 and is_fully_refunded = 0,
                ifnull(p.subtotal_excl_vat, 0),
                0
            ) as subtotal_excl_vat,
            iff(
                is_paid_online = 1 and is_fully_refunded = 0 and is_deleted = 0,
                ifnull(p.discount_incl_vat, 0),
                0
            ) as discount_incl_vat,
            -- should Is_Deleted be applied to the SubTotal too, needs confirmation
            iff(
                is_paid_online = 1 and is_fully_refunded = 0 and booking_duration = 60,
                1,
                0
            ) as duration60mins_count,
            iff(
                dayofweek(b.booking_date) in (5, 6) or b.booking_time >= '16:00:00', 1, 0
            ) as peak_booking_count,
            booking_duration,
            1 as bookings_count,

            iff(p.promo_code is null, 0, 1) as promo_code_count,
            p.session_id as payment_session_id,
            booking_duration_mins,
            box_end_time,
            p.refund_amount,
            P.Refund_Date,
            C.Post_code, 
            C.E_Mail

        from cte_stg_bookings as b
        left join cte_payment p on b.booking_reference = p.booking_reference
        left outer join cte_stg_customer as c on b.session_id = c.sessionid
        left outer join
            cte_stg_booking_source as bs
            on b.booking_source = bs.source
            and b.booking_type = bs.type
            where b.booking_reference is not null 

    )

select *
from cte_main

