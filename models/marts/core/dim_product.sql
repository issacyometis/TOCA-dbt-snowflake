with cte_stg_product as (

select * from {{ref('stg_epos__product')}}
) , 

cte_stg_sales_detail as (

select * from {{ref('stg_epos__sales_detail')}}
) ,

cte_Product as (

    select 
        md5(productid) as ProductKey, 
        productid as Product_id, 
        productname as Product_name,
        productgroupname as Product_group_name,
        producttypename as Product_Type_Name, 
        isvirtuallysellable as Is_virtually_sellable,
        isalcohol as Is_Alchol,
        0 as TaholaCreated
        from cte_stg_product
), 

cte_missing_product as (

select distinct
md5(m.product_id) as ProductKey, 
M.Product_id, 
M.product_name,
min(M.product_group_name) as product_group_name,
min(M.product_type_name) as product_type_name,
0 as Is_virtually_sellable,
0 as Is_Alchol,
1 as TaholaCreated
 from cte_stg_sales_detail as M
 left join cte_Product as P on M.Product_id = p.Product_id

 where coalesce(p.Product_id, -9999) = -9999
group by md5(m.product_id) , 
M.Product_id, 
M.product_name
) ,cte_main as (

select * from cte_Product
union 
select * from cte_missing_product

)


select * from cte_main