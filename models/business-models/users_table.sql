
with 
	user_value 
		as 
			(
				select 
					  o.user_id 
					, count(distinct o.order_id) as order_count
					, sum(o.gross_total_amount_dollars) as ltv
				from {{ref('orders_table')}} o
				where o.order_status_category = 'completed'
				group by 1 
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
			),
	user_dates
		as 
			(
				select 
					  o.user_id
					, min(o.order_created_at) as user_created_at
					, max(o.order_updated_at) as user_updated_at
				from {{ref('orders_table')}} o 
				group by 1 
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
	, ud.user_created_at
	, ud.user_updated_at

from user_value uv
left join user_locations ul 
	on ul.user_id = uv.user_id
left join user_dates ud 
	on ud.user_id = uv.user_id

{% if is_incremental() %}
	where ud.user_updated_at > (
			select max(t.user_updated_at)
			from {{ this }} t
		)  
{% endif %}


