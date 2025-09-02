with
    cte_source as (select * from {{ source("inputs", "MANAGER_COMMENTS_DAILY_SNAPSHOT_COMMENTS") }}),
    
    cte_latest as (select 
    Response_date, max(response_number) as response_number
    from cte_source
    group by Response_date
  
    ),
    
    
    cte_main as (

        select
        IFF(c.venue = 'Birmingham Bullring', 2, 1) as Site_ID, 
        to_date(DATEADD(s, (C.created_at/1000),'1970-01-01')) as Created_Date,
        to_time(DATEADD(s, (C.created_at/1000),'1970-01-01')) as Created_Time,
        C.Form_Name,
        C.response_number,
        C.Response_date,
        --to_date(Response_date) as Response_date, 
        to_date(DATEADD(s, (C.Submitted_at/1000),'1970-01-01')) as Submitted_Date,
        to_time(DATEADD(s, (C.Submitted_at/1000),'1970-01-01')) as Submitted_Time,
        C.SENIOR_MANAGER_ON_DUTY_AM,
        C.SENIOR_MANAGER_ON_DUTY_PM,
        C.MANAGERS_ON_DUTY_AM,
        C.MANAGERS_ON_DUTY_PM,
        C.DAILY_SNAPSHOT_COMMENTS_2 as DAILY_SNAPSHOT_COMMENTS,
        C.LEGEND_S_OF_THE_DAY as LEGENDS_OF_THE_DAY,
        C.TEAM_OVERVIEW,
        C.SALES_PERFORMANCE,
        C.SPBH,
        C.EVENTS_PROMOS,
        C.HAVE_THERE_BEEN_ANY_HEALTH_SAFETY_ISSUES_ON_SITE_TODAY as Health_And_Safety,
        C.DISCOUNT_COMMENTS



        from cte_source as C
        inner join cte_latest as L on C.Response_date = L.Response_date
        and C.response_number = L.response_number
   
    )

select *
from cte_main
