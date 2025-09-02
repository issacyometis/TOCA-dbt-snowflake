with 
    cte_source as (select * from {{ source('epos', 'sales_api') }}),

    cte_epos_Payments as (

    select 
    ifnull(siteID, 3957) as site_ID,
    f.value:EPOSTransactionID::int as EPOS_Transaction_ID,
    f.value:TenderAmount::float as Tender_Amount,
    f.value:TenderTypeID::int as Tender_Type_ID,
    f.value:TenderTypeName::string as Tender_Type_Name,
    f.value:EmployeeID::int as Employee_ID,
    f.value:EmployeeName::string as Employee_Name,
    f.value:TenderedDateTime::DateTime as Tendered_Date_Time,
    f.value:TenderID::int as Tender_ID
   
     from cte_source, table(flatten(PaymentInfos)) f

    )

  , cte_main as (

select 
site_id,
EPOS_Transaction_ID,
Tender_Amount,
 Tender_Type_ID,
 Tender_Type_Name,
 Employee_ID,
 Employee_Name,
  to_date(Tendered_Date_Time) as Tender_date,
  to_time(Tendered_Date_Time) as Tender_time,
 Tender_ID



 from cte_epos_payments



  )



select 
*
 from cte_main