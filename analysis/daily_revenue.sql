SELECT 
      DATE(DATETIME_TRUNC(o.order_created_at, DAY)) as order_day
    , ROUND(SUM(o.order_amount_total_dollars),2) as daily_revenue
FROM `fishtown-interview.dbt_ynewman.orders_table` o
group by 1 
order by 1 asc 