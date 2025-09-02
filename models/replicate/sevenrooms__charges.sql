{{ config(alias="stg_sevenrooms__charges") }}
with
    cte_source as (
        select * from {{ source("sevenrooms", "CHARGES") }}
    ), 

    cte_main as ( 
   select 
   ID as Insert_ID, 
   Insert_date, 
   replace(value:additional_fee,'"', '') as additional_fee,
   replace(value:additional_fee_tax,'"', '') as additional_fee_tax,  
   replace(value:amount,'"', '') as amount,  
   replace(value:base_amount,'"', '') as base_amount,  
   replace(value:brand,'"', '') as brand,  
   replace(value:charged,'"', '') as charged,  
   replace(value:entity_date,'"', '') as entity_date,  
   replace(value:entity_id,'"', '') as entity_id,  
   replace(value:failure_code,'"', '') as failure_code,  
   replace(value:failure_message,'"', '') as failure_message,  
   replace(value:gratuity_amount,'"', '') as gratuity_amount,  
   replace(value:id,'"', '') as id,  
   replace(value:is_info_request,'"', '') as is_info_request,  
   replace(value:is_refund,'"', '') as is_refund,  
   replace(value:last_4,'"', '') as last_4,  
   replace(value:notes,'"', '') as notes,  
   replace(value:original_amount,'"', '') as original_amount,  
   replace(value:processing_fee,'"', '') as processing_fee,  
   replace(value:promo_code,'"', '') as promo_code,  
   replace(value:promo_discount_amount,'"', '') as promo_discount_amount,  
   replace(value:service_charge_amount,'"', '') as service_charge_amount,  
   replace(value:status,'"', '') as status,
   replace(value:tax_amount,'"', '') as tax_amount,
   replace(value:transaction_id,'"', '') as transaction_id,
   replace(value:transaction_type,'"', '') as transaction_type,
   replace(value:upsell_amount,'"', '') as upsell_amount,
   replace(value:venue_group_client_id,'"', '') as venue_group_client_id,
   replace(value:venue_id,'"', '') as venue_id

from cte_source 
,lateral flatten(input => JSON_DATA:data:charges) d


  
     )

     


    select 
    
    * 
   from cte_main