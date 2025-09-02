


 with cte_dbt_utils as (
 {{ dbt_date.get_date_dimension("2015-01-01", "2026-12-31") }} 
 )
, 

cte_current_date as (
select * from cte_dbt_utils
where date_day = to_date(getdate())
),


cte_detail as (
 select 
 
    d.date_day as Trans_Date,
    d.Day_of_week_iso as Day_Of_Week_No,
    d.Day_of_Week_name as Day_Name,
    d.day_of_week_name_short as Day_Short_Name,
    d.Day_of_Month as Day_Of_Month,
    d.Month_Name as Month_Name,
    d.Month_Name_Short as Month_Name_Short,
    d.Year_Number as Year_Number,
    d.Day_of_Year as Day_Of_Year,
    d.iso_Week_of_Year as Week_Of_Year,
    datediff(Year, getdate(), d.date_day) as Relative_Year,
    datediff(Quarter, getdate(), d.date_day) as Relative_Quarter,
    datediff(Month, getdate(), d.date_day) as Relative_Month,
    datediff(Week, getdate(), d.date_day) as Relative_Week,
    datediff(day, getdate(), d.date_day) as Relative_day

  from cte_dbt_utils as d
  
) ,

cte_main as (
  select 
   *,
   iff(Relative_Year = 0 and Relative_day <= 0, 1, 0) as Is_YTD,
   iff(Relative_Quarter = 0 and Relative_day <= 0, 1, 0) as Is_QTD,
   iff(Relative_Month = 0 and Relative_day <= 0, 1, 0) as Is_MTD,
   iff(Relative_Week = 0 and Relative_day <= 0, 1, 0) as Is_WTD
   

  from 
    cte_detail
)

select * from cte_main

