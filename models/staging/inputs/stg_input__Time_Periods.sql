with cte_seed_time_periods as (

select * from {{ref('Time_Periods')}}
) , 

cte_main as (select 
concat(left(day,3),hour) as Unique_Day_Hour,
day as Day_Of_Week, 
Hour as Hour_Of_Day, 
Period_Type as Period_Type,
Period_sort as Period_Sort,
Hours_Available as Hours_Available




 from cte_seed_time_periods )


select * from cte_main