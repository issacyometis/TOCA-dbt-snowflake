with 
    cte_source as (select * from {{ source('Booking_Customers', 'TAHOLA_TOCA_AUTH_TOKEN') }}),

    cte_stg as (

        select 

C.UUID as SessionID, 
try_parse_json(raw):name::string as Full_Name,
try_parse_json(raw):email::string as E_Mail,
try_parse_json(raw):mobile::string as Mobile,
try_parse_json(raw):dob::string as DOB,
try_parse_json(raw):marketing::string as Marketing_JSON,
try_parse_json(raw):marketing:gdpr::boolean as Marketing_GDPR,
try_parse_json(raw):marketing:newsletters::boolean as Marketing_Newsletters,
try_parse_json(raw):marketing:share_pics_videos::boolean as Marketing_Shared_Pics_Videos, 
try_parse_json(raw):postcode::string as Post_code, 
c.created as Created_Date_Time, 
c.username ,
md5(try_parse_json(raw):email::string) AS MD5_Email,
try_parse_json(raw):has_under_18::string as Has_Under_18
        
        
        
         from cte_source as c
         where c.raw is not null 
    ), 
cte_main as (
select *, try_to_date(replace(dob,'/','-')) as Date_of_birth from cte_stg

)


    select * from cte_main
   