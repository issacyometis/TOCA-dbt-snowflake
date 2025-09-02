with cte_stg_payment as (

select * from {{ref('stg_booking__payment')}}
) , 


cte_main as (

select 
Distinct 
MD5(coalesce(Payment_Provider,'N/A')) as Payment_Provider_Key, 
coalesce(Payment_Provider,'N/A') as Payment_Provider
from cte_stg_payment

)


select * from cte_main