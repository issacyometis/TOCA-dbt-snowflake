with
    cte_stg_sales_header as (select * from {{ ref("stg_epos__sales_header") }}),
    cte_stg_sales_detail as (select * from {{ ref("stg_epos__sales_detail") }}),
    cte_dim_tenders as (select * from {{ ref("dim_tender") }}),
    cte_fact_tender as (select * from {{ ref("fact_tender") }}),
    cte_stg_tender_detail as (select * from {{ ref("stg_epos__sales_payment") }}),
    cte_dim_product as (select * from {{ ref("dim_product") }}),
    cte_bussiness_drivers as (select * from {{ref("dim_business_drivers")}}),
    cte_events_packages as (

        select distinct
            f.epos_transaction_id,
            f.siteID,
            iff(t.tender_type_id = 1008, 1, 0) as is_event_paid,
            iff(t.tender_type_id = 1014, 1, 0) as is_package_paid
        from cte_fact_tender as f
        inner join cte_dim_tenders as t on f.tender_type_key = t.tender_type_key
        where t.tender_type_id in (1014, 1008)
    ),
    cte_main as (


        select
            b.sk_site,
            coalesce(p.productkey, '-9999') as productkey,
            md5(
                concat(dayname(h.sale_date), date_part(hour, h.sale_time))
            ) as day_time_period_key,
            h.epos_transaction_id,
            h.business_date,
            h.sale_date,
            h.sale_time,
            h.table_number,
            h.revenue_center,
            h.open_date_time,
            h.closed_date_time,
            h.employee_opened_bill_id,
            h.employee_opened_bill as employee_opened,
            h.employee_closed_bill_id,
            h.employee_closed_bill as employee_closed,
            h.bill_paid,
            d.epos_transaction_item_id,

            case
                when d.is_voided = 'false' and d.is_error_correct = 'false'
                then d.qty_sold
                else 0
            end as qty_sold,
            case
                when d.is_voided = 'false' and d.is_error_correct = 'false'
                then d.unit_quantity
                else 0
            end as unit_quantity,
            case
                when d.is_voided = 'false' and d.is_error_correct = 'false'
                then d.cost_price
                else 0
            end as cost_price,
            case
                when d.is_voided = 'false' and d.is_error_correct = 'false'
                then d.expected_gross_amt
                else 0
            end as expected_gross_sales,
            case
                when d.is_voided = 'false' and d.is_error_correct = 'false'
                then d.actual_gross_amt
                else 0
            end as actual_gross_sales,
            case
                when d.is_voided = 'false' and d.is_error_correct = 'false'
                then d.taxable_net_amt
                else 0
            end as net_sales,
            case
                when d.is_voided = 'false' and d.is_error_correct = 'false'
                then d.tax_amt
                else 0
            end as vat,
            d.is_voided,
            case
                when d.is_voided = 'true' then d.qty_sold else 0
            end as voided_qty_sold,
            case
                when d.is_voided = 'true' then d.unit_quantity else 0
            end as voided_unit_quantity,
            case
                when d.is_voided = 'true' then d.cost_price else 0
            end as voided_cost_price,
            case
                when d.is_voided = 'true' then d.expected_gross_amt else 0
            end as voided_expected_gross_sales,
            case
                when d.is_voided = 'true' then d.actual_gross_amt else 0
            end as voided_actual_gross_sales,
            case
                when d.is_voided = 'true' then d.taxable_net_amt else 0
            end as voided_net_sales,
            case when d.is_voided = 'true' then d.tax_amt else 0 end as voided_vat,
            d.is_error_correct,
            case
                when d.is_error_correct = 'true' then d.qty_sold else 0
            end as error_correct_qty_sold,
            case
                when d.is_error_correct = 'true' then d.unit_quantity else 0
            end as error_correct_unit_quantity,
            case
                when d.is_error_correct = 'true' then d.cost_price else 0
            end as error_correct_cost_price,
            case
                when d.is_error_correct = 'true' then d.expected_gross_amt else 0
            end as error_correct_expected_gross_sales,
            case
                when d.is_error_correct = 'true' then d.actual_gross_amt else 0
            end as error_correct_actual_gross_sales,
            case
                when d.is_error_correct = 'true' then d.taxable_net_amt else 0
            end as error_correct_net_sales,
            case
                when d.is_error_correct = 'true' then d.tax_amt else 0
            end as error_correct_vat,
            ifnull(t.is_event_paid, 0) as is_event_paid,
            ifnull(t.is_package_paid, 0) as is_package_paid

        from cte_stg_sales_header as h
        inner join
            cte_stg_sales_detail as d on h.epos_transaction_id = d.epos_transaction_id
            and h.siteID = d.siteID
        left join cte_dim_product as p on p.product_id = d.product_id
        left join
            cte_events_packages as t on h.epos_transaction_id = t.epos_transaction_id
            and h.siteID = t.siteID
        left join 
            cte_bussiness_drivers as b on h.siteID = b.epos_id
    )



select *
from cte_main
