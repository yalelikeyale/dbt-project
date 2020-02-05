
with 
	orders_base 
		as 
			(
				select
					  o.order_id
					, o.user_id
					, {{convert_local('o.created_at')}} as order_created_at
					, {{convert_local('o.updated_at')}} as order_updated_at
					, {{convert_local('o.shipped_at')}} as order_shipped_at
					, o.currency as order_currency
					, o.status as order_status
					, CASE
						WHEN o.status IN ('paid','completed','shipped') 
							THEN 'completed'
						ELSE o.status
						END AS order_status_category
					, o.shipping_method as order_shipping_method
					, {{convert_dollar('o.amount_total_cents')}} as order_amount_total_dollars
					, ROW_NUMBER() OVER( PARTITION BY o.user_id ORDER BY o.created_at) as order_sequence

				from {{ source('ecommerce','orders') }} o
			)

select 
	  * 
	, CASE
		WHEN o.order_sequence = 1 
		THEN 1
		ELSE 0
	  END as first_order

from orders_base o 

