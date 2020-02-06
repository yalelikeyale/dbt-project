SELECT 
      DATE(DATETIME_TRUNC(o.order_created_at, MONTH)) as order_month
    , ROUND(SUM(o.order_amount_total_dollars),2) as monthly_revenue
FROM `fishtown-interview.dbt_ynewman.orders_table` o
group by 1 
order by 1 asc 