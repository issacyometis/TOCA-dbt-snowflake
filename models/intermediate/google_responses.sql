with cte_stg as ({{dbt_utils.snowflake__deduplicate(
    ref("stg_inputs__google_form_response"), 
    "responce_id, question_id",
    "insert_date desc"
    )
}}),



--with cte_stg as (select * from {{ref("stg_inputs__google_form_response")}}),

cte_site as (select * from cte_stg
where Question ='Site'

),

cte_responces as (select distinct responce_id, submitted_date, submitted_time, submitted_at from cte_stg
where Question <>'Site'
),

cte_main as (select R.*, 
ifnull(S.question_answer,'London O2') as site,
S.question_answer as org_site
from cte_responces as R
left outer join cte_site as S on R.responce_id = s.responce_id


)

select * from cte_main 
