select 
    ROUND((sum(case when o.first_order = 1 then o.order_amount_total_dollars else 0 end) / sum(o.order_amount_total_dollars)),2) as
from `fishtown-interview.dbt_ynewman.orders_table` o
where o.order_status_category = 'completed'