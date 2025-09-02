with 
    cte_source as (select * from {{ source('Booking', 'TOCA_PAYMENTS_PAYMENT') }}),


cte_stg as (

select 
	PAYMENTID as Payment_Id,
	PAYMENTPROVIDER as Payment_Provider,
	BOOKINGSOURCE as Booking_Source,
	BOOKINGREF  as Booking_reference,
	REFUNDID as Refund_Id,
	SESSIONID as Session_Id,

	BOOKINGTIMESTAMP as Booking_Timestamp,
    to_date(BOOKINGTIMESTAMP) as Booking_Date,
    to_time(BOOKINGTIMESTAMP) as Booking_Time,

	REFUNDTIMESTAMP as Refund_Timestamp,
	to_date(DATEADD(s, REFUNDTIMESTAMP,'1970-01-01')) as Refund_Date,
	to_time(DATEADD(s, REFUNDTIMESTAMP,'1970-01-01')) as Refund_Time,	

	TRANSACTIONTIMESTAMP as Transaction_Timestamp,

	CREATEDAT as CreateDate_Timestamp,
    to_date(CREATEDAT) as Create_Date,
    to_time(CREATEDAT) as Create_Time,

	UPDATEDAT as UpdateDate_Timestamp,
    to_date(UPDATEDAT) as Update_Date,
    to_time(UPDATEDAT) as Update_Time,

	DELETEDAT as DeleteDate_Timestamp,
    to_date(DELETEDAT) as Delete_Date,
    to_time(DELETEDAT) as Delete_Time,
	CANCELLATIONTIMESTAMP as Cancellation_Timestamp,

	ISPAID as Is_Paid,
	PROMOCODE as Promo_Code,
	ISCANCELLED as Is_Cancelled,
	REASON as Reason,

    TOTALINCVAT/100 as Total,
	VAT/100 as VAT,
	TOTALPP/100  as Total_PP,
	SUBTOTALEXVAT/100 as SubTotal_Excl_Vat,
	PRICEINCVAT/100 as Price_Incl_Vat,
	IFF(to_date(UPDATEDAT) <'2021-09-24', REFUNDAMOUNT/100, REFUNDAMOUNT ) as Refund_Amount,
	DISCOUNTINCVAT/100 as Discount_Incl_Vat

    --iff(TOTALINCVAT = REFUNDAMOUNT,1,0)        as Is_Fully_Refunded
-- could this just be total - refund?

 from cte_source

), cte_main as (

select *, iff(Total = Refund_Amount,1,0)        as Is_Fully_Refunded from cte_stg

)

select * from cte_main

