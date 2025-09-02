with
    cte_source as (select * from {{ source("inputs", "GOOGLE_FORMS") }}),
    cte_seed as (select * from {{ref('google_questions')}}),
    
    cte_hevo as (
    select  
    json_data as responses,
    insert_date

    from cte_source

  
    ),
    
    
    cte_head as (

        select 
  
  h.value:createTime as created_at,
  h.value:lastSubmittedTime as submitted_at,
  h.value:respondentEmail as respondent_email,
  h.value:responseId as responce_id, 
  h.value:answers as json_answers,
  cte_hevo.insert_date
  
  from cte_hevo, LATERAL FLATTEN(INPUT => responses:responses) h

   
    ), 

    cte_questions as (

        select cte_head.*, 
        d.value:questionId as question_id,
        d.value:textAnswers:answers as json_question_answer
         from cte_head, lateral flatten( INPUT => json_answers ) d
    ), 

    cte_answer as (
    
    select cte_questions.*, 
        q.value:"value" as question_answer
    
     from cte_questions, lateral flatten( INPUT => json_question_answer ) q

    ),

    cte_stg as (select 
    to_timestamp(a.created_at) as created_at,
    to_date(a.created_at) as created_date,
    to_time(to_timestamp(a.created_at)) as created_time,
    to_timestamp(a.submitted_at) as submitted_at,
    to_date(a.submitted_at) as submitted_date,
    to_time(to_timestamp(a.submitted_at)) as submitted_time,
    a.respondent_email::string as respondent_email,
    a.responce_id::string as responce_id,
    a.question_id::string as question_id,
    s.Question as question,
    a.question_answer::string as question_answer,
    a.insert_date
    
    
     from cte_answer as a 
     left outer join cte_seed as s on a.question_id::string = s.question_id
    ),

    cte_main as (

select  

*

from cte_stg 


    )

select *
from cte_main

