with cte_stg_day_time_period as (

select * from {{ref('stg_input__Time_Periods')}}
) , 





cte_main as (

select 
md5(Unique_Day_Hour) as Day_Time_Period_Key,
Unique_Day_Hour,
Day_Of_Week,
Hour_Of_Day,
Period_Type,
Period_Sort,
Hours_Available





 from cte_stg_day_time_period

)

select * from cte_main