with cte_stg_payment as (

select * from {{ref('stg_booking__payment')}}
) ,


cte_main as (

select 
--Keys
	md5(b.Payment_Provider) as Payment_Provider_key, 

--Dates
    Booking_Date,
    Create_Date,
    Update_Date,
    Delete_Date,
	Refund_Date,

--Times
    Booking_Time,
    Create_Time,
    Update_Time,
    Delete_Time,    
	Refund_Time,	
	
--Non Dimensional Attributes
	Payment_Id,
	Booking_Source,
	Booking_Reference,
	Refund_Id,
	Session_Id,
	Is_Cancelled,
	Reason,
	Is_Paid,
	Promo_Code,
	Is_Fully_Refunded,    

--Measures
    Total,
	Total / 100 as Payment_Total,
	VAT,
	VAT / 100 as Payment_VAT,
	Total_PP,
	Total_PP / 100 as Payment_Total_PP,
	SubTotal_Excl_Vat,
	SubTotal_Excl_Vat / 100 as Payment_SubTotal_Excl_Vat,
 	Price_Incl_Vat,
	Price_Incl_Vat / 100 as Payment_Price_Incl_Vat,
	Refund_Amount,
	Refund_Amount / 100 as Payment_Refund_Amount,
	Discount_Incl_Vat,
	Discount_Incl_Vat / 100 as Payment_Discount_Incl_Vat

from cte_stg_payment as B 

)

select * from cte_main