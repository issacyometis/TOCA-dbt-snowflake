with
    cte_stg_bookings as (select * from {{ ref("base_sevenrooms__reservations") }}),
    cte_stg_payments as (select * from {{ ref("fact_sevenrooms_payments") }}),
    cte_site as (select * from {{ ref("dim_business_drivers") }}),
    cte_stg_upgrade as (select * from {{ref("fact_sevenrooms_upgrades")}}),
    cte_tbl_tags as (select * from {{ref("tbl_sevenrooms_tags")}}), 

cte_upgrade as (
select reference, sum(Total_upgrade) as Total_upgrade from cte_stg_upgrade
group by reference

)
,

cte_Payment_summary as (

select reference, 
sum(iff(is_refund = 'true', amount_pounds *-1, amount_pounds)) as Total_Amount, 
sum(iff(is_refund = 'true', amount_pounds *-1, 0)) as Refund_Amount, 
sum(iff(is_refund = 'true', 0, amount_pounds)) as Paid_Amount,
sum(payment_count) as Number_of_Payments,
max(iff(is_refund = 'true', CHARGED_DATE, null)) as Refund_date
--is_refund, sum(Amount) as Amount,sum(original_amount) as original_amount, sum(payment_count) as payment_count

from cte_stg_payments
GRoup by reference
), 

cte_exclude_bookings as (
select distinct reference from cte_tbl_tags
where tag in('EUROS 2024', 'EXCLUDE')

),

    cte_main as (

        select
            s.sk_site,
            b.client_id as sk_clients,
            b.*,
            b.total_net_payment/1.20 as total_net_payment_excluding_vat,
            iff(
                dayofweek(b.fixture_date) in (6, 7) or b.fixture_time > '16:00:00', 1, 0
            ) as peak_booking_count, p.Refund_Amount, p.Refund_date, IFF(Total_Amount=0, 1, 0) as Is_Fully_Refunded
      , replace(substr(REGEXP_SUBSTR(b.notes, 'Ref\\W+\\w+'),5, 10),'| ', '') as Legacy_booking_reference,
      u.Total_upgrade
        from cte_stg_bookings as b 
        left outer join cte_Payment_summary as P on p.reference = b.reference
        left outer join cte_site as s on b.Venue_ID = s.SEVENROOMS_ID
        left outer join cte_upgrade as u on b.reference = u.reference
        where b.reference not in (Select reference from cte_exclude_bookings)
    )

select *
from cte_main


