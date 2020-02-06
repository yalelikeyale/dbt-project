select 
      FORMAT_DATE('%A', DATE(o.order_created_at)) AS weekday_name_full  
    , sum(o.order_amount_total_dollars)
from `fishtown-interview.dbt_ynewman.orders_table` o
group by 1