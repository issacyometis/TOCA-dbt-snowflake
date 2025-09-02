with 
    cte_booking as (
        select * from {{ ref('fact_booking') }}

    ),

    cte_date as (
        select * from {{ ref('dim_date') }}

    )


select 

--d.Trans_Date,
sum (iff(d.Relative_day = 1, b.SubTotal_Excl_Vat, 0) ) as SubTotal_Excl_Vat_Yesterday,
sum (iff(d.Relative_day = 1, b.Discount_Incl_Vat, 0) ) as Discount_Incl_Vat_Yesterday,
sum (iff(d.Relative_day = 1, Duration60Mins_Count, 0) ) as Duration60Mins_Count_Yesterday,
sum (iff(d.Relative_day = 1, Peak_Booking_Count, 0) ) as Peak_Booking_Count_Yesterday,
sum (iff(d.Relative_day = 1, Promo_Code_Count, 0) ) as Promo_Code_Count_Yesterday,
sum (iff(d.Relative_day = 1, Bookings_Count, 0) ) as Bookings_Count_Yesterday,

sum (iff(d.Is_YTD = 1, b.SubTotal_Excl_Vat, 0) ) as SubTotal_Excl_Vat_YTD,
sum (iff(d.Is_YTD = 1, b.Discount_Incl_Vat, 0) ) as Discount_Incl_Vat_YTD,
sum (iff(d.Is_YTD = 1, Duration60Mins_Count, 0) ) as Duration60Mins_Count_YTD,
sum (iff(d.Is_YTD = 1, Peak_Booking_Count, 0) ) as Peak_Booking_Count_YTD,
sum (iff(d.Is_YTD = 1, Promo_Code_Count, 0) ) as Promo_Code_Count_YTD,
sum (iff(d.Is_YTD = 1, Bookings_Count, 0) ) as Bookings_Count_YTD

from cte_booking b 
join cte_date d 
on b.Booking_Date = d.trans_date
--group by d.Trans_Date
--order by d.Trans_Date desc