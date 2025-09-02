
with cte_stg as ({{dbt_utils.snowflake__deduplicate(
    ref("stg_inputs__google_form_response"), 
    "responce_id, question_id",
    "insert_date desc"
    )
}}),


cte_google as (select * from {{ref("google_responses")}}),

cte_bussiness_drivers as (select * from {{ref("dim_business_drivers")}}),


cte_stage as (

    select g.*, 
    case when g.site ='London O2' then 1 when g.site = 'Birmingham Bullring' then 2 end as site_id  ,

    s.created_at,
    s.question_id,
    s.question,
    s.question_answer
    from cte_google as g 
    inner join cte_stg as S on g.responce_id = s.responce_id
    where s.question <> 'Site'
),


cte_main as (
    select 
b.sk_site as sk_site,
CASE WHEN submitted_time <'05:00:00' then dateadd(day, -1, Submitted_date) 
else Submitted_date end as Business_date,
c.*

 from cte_stage as c
left outer join cte_bussiness_drivers as b on c.Site = b.google_form_id)






/*


 cte_stg_Manager_comments as (

select * from {{ref('stg_inputs__manager_comments')}}
) ,
cte_bussiness_drivers as (select * from {{ref("dim_business_drivers")}}),


cte_main as (

select 
b.sk_site as sk_site,
CASE WHEN submitted_time <'05:00:00' then dateadd(day, -1, Submitted_date) 
else Submitted_date
end as Business_date,
Submitted_date, 
submitted_time,
SENIOR_MANAGER_ON_DUTY_AM,
SENIOR_MANAGER_ON_DUTY_PM,
MANAGERS_ON_DUTY_AM,
MANAGERS_ON_DUTY_PM,
DAILY_SNAPSHOT_COMMENTS,
LEGENDS_OF_THE_DAY,
TEAM_OVERVIEW,
SALES_PERFORMANCE,
SPBH,
EVENTS_PROMOS,
Health_And_Safety,
DISCOUNT_COMMENTS


 from cte_stg_Manager_comments as c
 left outer join cte_bussiness_drivers as b on c.Site_ID = b.TOCA_REPORTING_ID
 

)
*/

select * from cte_main



--where question = 'Site'