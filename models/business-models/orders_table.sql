
with 
	order_device  
		as 
			(
				SELECT
					DISTINCT d.order_id,
					FIRST_VALUE(d.device_type IGNORE nulls) OVER 
						(
							PARTITION BY d.order_id
							ORDER BY d.device_created_tstmp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
						) AS payment_device_type
				FROM {{ref('devices')}} d
				WHERE d.event_type = 'order'
			),
	payment_totals
		as 
			(
				select
					  p.order_id
					, sum(CASE WHEN p.payment_status = 'completed' THEN p.payment_tax_amount_dollars ELSE 0 END) as gross_tax_amount_dollars
					, sum(CASE WHEN p.payment_status = 'completed' THEN p.payment_amount_dollars ELSE 0 END) as gross_amount_dollars
					, sum(CASE WHEN p.payment_status = 'completed' THEN p.payment_amount_shipping_dollars ELSE 0 END) as gross_shipping_amount_dollars
					, sum(CASE WHEN p.payment_status = 'completed' THEN p.payment_tax_amount_dollars + p.payment_amount_dollars + p.payment_amount_shipping_dollars ELSE 0 END) as gross_total_amount_dollars
				FROM {{ref('payments')}} p 
				GROUP BY 1
			)
select 
	  o.order_id
	, o.user_id
	, o.order_status
	, o.order_status_category
	, o.order_currency
	, o.first_order
	, o.order_sequence
	, od.payment_device_type
	, a.country_type
	, pt.gross_amount_dollars
	, pt.gross_tax_amount_dollars
	, pt.gross_shipping_amount_dollars
	, pt.gross_total_amount_dollars
	, o.order_amount_total_dollars
	, o.order_created_at
	, o.order_updated_at
	, o.order_shipped_at

from {{ref('orders')}} o
left join {{ref('addresses')}} a
	on a.order_id = o.order_id
left join order_device od 
	on od.order_id = o.order_id 
left join payment_totals pt 
	on pt.order_id = o.order_id

{% if is_incremental() %}
    where o.order_id in (
        select distinct o.order_id
        from {{ref('orders')}} o
        where o.order_updated_at > (
          select max(t.order_updated_at)
          from {{ this }} t)
        )
{% endif %}



