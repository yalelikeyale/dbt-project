select 
      FORMAT_DATE('%A', DATE(o.order_created_at)) AS weekday_name_full  
    , sum(o.order_amount_total_dollars) as weekday_revenue
from `fishtown-interview.dbt_ynewman.orders_table` o
where o.order_status_category = 'completed'
group by 1