select AVG(o.first_order)
from `fishtown-interview.dbt_ynewman.orders_table` o
where o.order_status_category = 'completed'