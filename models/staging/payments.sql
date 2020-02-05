

select 
	  p.payment_id
	, p.order_id
	, {{convert_local('p.created_at')}} as payment_created_at
	, p.status
	, {{convert_dollar('p.tax_amount_cents')}} as payment_tax_amount_dollars
	, {{convert_dollar('p.amount_cents')}} as payment_amount_dollars
	, {{convert_dollar('p.amount_shipping_cents')}} as payment_amount_shipping_dollars
	, p.payment_type

from {{ source('ecommerce','payments') }} p