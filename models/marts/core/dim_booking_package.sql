with cte_stg_package as (

select * from {{ref('stg_booking__package')}}
) , 

cte_stg_category as (

select * from {{ref('stg_booking__category')}}
) ,

cte_main as (

select  P.Package_ID
, P.Package_Code
, P.Package_Name
, P.Package_Description
, P.Price
, P.Price_Per_Person
, P.Hint
, P.Category_ID
, C.Category_Name
, C.Priority
from cte_stg_package as P 
left join cte_stg_category as C on p.Category_id = C.Category_id

)

select * from cte_main