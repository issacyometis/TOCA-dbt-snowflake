with cte_stg_customer as (

select * from {{ref('stg_booking__booking_customer')}}
) , 


cte_main as (

select 
 
SessionID as Customer_Key,
*,

CASE WHEN e_mail like '%@toca%' then 1 else 0 end as IS_TOCA_Employee

 from cte_stg_customer

)

select * from cte_main