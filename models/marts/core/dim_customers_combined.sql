with
    cte_source as (

        {{
            dbt_utils.snowflake__deduplicate(
                ref("stg_booking__booking_customer"),
                "E_Mail",
                "Created_Date_Time desc",
            )
        }}

    ),
    cte_bookings as (select * from {{ ref("stg_booking__booking_customer") }}),
    cte_stg_seven_cust as (select * from {{ ref("dim_sevenrooms_customer") }}),

    cte_count_bookings as (
        select e_mail, count(distinct sessionid) as number_of_bookings
        from cte_bookings
        group by e_mail
    ),
     
     cte_sv_cust as (
select email, max(created_at) as created_at from cte_stg_seven_cust
group by email

     )
    ,
    cte_main as (

        select
            s.md5_email as sk_customer,
            s.full_name,
            s.e_mail,
            s.mobile,
            s.date_of_birth,
            s.marketing_gdpr,
            s.marketing_newsletters,
            s.marketing_shared_pics_videos,
            s.post_code,
            s.md5_email,
            sum(c.number_of_bookings + ifnull(sc.total_visits,0)) as Number_of_Bookings
        from cte_source as s
        left outer join cte_count_bookings as c on s.e_mail = c.e_mail
        left outer join cte_stg_seven_cust as SC on s.e_mail = sc.email
group by s.full_name,
            s.e_mail ,
            s.mobile,
            s.date_of_birth,
            s.marketing_gdpr,
            s.marketing_newsletters,
            s.marketing_shared_pics_videos,
            s.post_code,
            s.md5_email

union 
        select 
        md5(cte_stg_seven_cust.email) as sk_customer, 
        concat(first_name, ' ', last_name) as Full_Name, 
        cte_stg_seven_cust.email,  
        phone_number as Mobile, 
        Null as Date_of_birth, 
        marketing_opt_in as marketing_gdpr,
        null as marketing_newsletters,
        null as marketing_shared_pics_videos,
        post_code, 
        md5(cte_stg_seven_cust.email) as MD5_Email,
        sum(total_visits) as Number_of_Bookings
        from cte_stg_seven_cust
        inner join cte_sv_cust as sv on cte_stg_seven_cust.email = sv.email and cte_stg_seven_cust.created_at = sv.created_at
        where cte_stg_seven_cust.email not in (select e_mail from cte_source)
        group by 
        concat(first_name, ' ', last_name) , 
        cte_stg_seven_cust.email,  
        phone_number ,
        marketing_opt_in ,
        post_code, 
        md5(cte_stg_seven_cust.email) 
            
    ) 



select *
from cte_main
