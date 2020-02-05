
with 
	user_value 
		as 
			(
				select 
					  o.user_id 
					, count(distinct o.order_id) "order_count"
					, sum(o.gross_total_amount_dollars) "ltv"
				from {{ref('orders_table')}} o
				where o.order_status_category = 'completed' 
			), 
	user_locations
		as 
			(
				select 
					  DISTINCT a.user_id 
					, LAST_VALUE(a.address IGNORE nulls) OVER 
						(
							PARTITION BY a.user_id
							ORDER BY o.order_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
						) AS recent_address
				from {{ref('addresses')}} a
				join {{ref('orders')}} o 
					on o.order_id = a.order_id
			)

select 
	  uv.user_id
	, ul.recent_address
	, CASE 
		WHEN uv.order_count > 1 
			THEN 1
		ELSE 0
		END as return_customer
	, uv.order_count 
	, uv.ltv 

from user_value uv
left join user_locations ul 
	on ul.user_id = uv.user_id


