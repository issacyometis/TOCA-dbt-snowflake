with
    cte_stg_sales_header as (select * from {{ ref("stg_epos__sales_header") }}),
    cte_stg_sales_payment as (select * from {{ ref("stg_epos__sales_payment") }}),
    cte_bussiness_drivers as (select * from {{ref("dim_business_drivers")}}),
    cte_main as (

        select
            b.sk_site,
            md5(p.tender_type_id) as tender_type_key,
            md5(
                concat(dayname(h.sale_date), date_part(hour, h.sale_time))
            ) as day_time_period_key,
            p.epos_transaction_id,
            p.tender_id,
            p.tender_type_name,
            h.sale_date,
            h.sale_time,
            p.tender_date,
            p.tender_time,
            p.employee_id,
            p.employee_name,
            p.tender_amount,
            P.site_ID as siteid




        from cte_stg_sales_payment as p
        left outer join
            cte_stg_sales_header as h on p.epos_transaction_id = h.epos_transaction_id
            and p.site_id = h.siteID
        left join 
            cte_bussiness_drivers as b on h.siteID = b.epos_id
    )

select *
from cte_main
