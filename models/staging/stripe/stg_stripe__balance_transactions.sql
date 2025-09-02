with
    cte_source as (
        select * from {{ source("stripe", "balance_transaction") }}
    ), 

    cte_main as (select 
    /*
    to_date(DATEADD(s, c.created,'1970-01-01')) as Created_date,
    to_time(DATEADD(s, c.created,'1970-01-01')) as Created_time,
    to_date(DATEADD(s, c.available_on,'1970-01-01')) as available_date,
    to_time(DATEADD(s, c.available_on,'1970-01-01')) as available_time,
    */
    to_date(to_timestamp_tz(c.created)) as Created_date,
    to_time(to_timestamp_tz(c.created)) as Created_time,
    to_date(to_timestamp_tz(c.available_on)) as available_date,
    to_time(to_timestamp_tz(c.available_on)) as available_time,


    C.ID as Transaction_id,
    c.Source as Source_id,
    C.Status,
    C.Type,
    C.Description,
    C.Currency,
    c.Reporting_category,
    c.fee/100 as Fee,
    C.net/100 as Net,
    c.amount/100 as Amount

    
    
    
    
     from cte_source as C)


    select * from cte_main