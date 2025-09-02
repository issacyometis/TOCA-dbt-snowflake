with 
    cte_source as (select * from {{ source('Booking', 'TOCA_PACKAGES_PACKAGE') }}),


cte_main as (

select ID as Package_ID,
category_ID as Category_ID,
CODE as Package_Code,
Name as Package_Name,
Description as Package_description,
Price as Price,
Price_Per_Person as Price_Per_Person, 
Hint as Hint
 from cte_source

)

select * from cte_main