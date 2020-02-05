

select 
	  a.order_id
	, a.user_id
	, a.name as user_name
	, a.address 
	, a.state
	, a.country_code
	, CASE
		WHEN a.country_code IS NULL 
			THEN 'Null country'
		WHEN a.country_code = 'US' 
			THEN 'US'
		WHEN a.country_code != 'US' 
			THEN 'International'
		END AS country_type

from {{source('ecommerce','addresses')}} a 