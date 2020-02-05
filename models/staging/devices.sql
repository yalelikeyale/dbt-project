with 
	device_1
		as 
			(
				select 
					  cast(d.type_id as int64) as order_id
					, d.device
					, d.type 
					, {{get_date('d.created_at')}} as created_date
					, {{get_seconds('d.created_at')}} as created_seconds
					, {{get_date('d.updated_at')}} as updated_date
					, {{get_seconds('d.updated_at')}} as updated_seconds

				from {{ source('ecommerce','devices') }} d
			),
	device_2
		as 
			(
				select
					  d.order_id
					, d.device 
					, d.type 
					, {{build_timestamp(d.created_date)}} as created_stmp
					, d.created_seconds
					, {{build_timestamp(d.updated_date)}} as updated_stmp
					, d.updated_seconds
				from device_1 d 
			)
select 
	  d.order_id
	, d.device 
	, d.type as event_type
	, CASE
		WHEN d.device = 'web' 
			THEN 'desktop'
		WHEN d.device IN ('ios-app', 'android-app') 
			THEN 'mobile-app'
		WHEN d.device IN ('mobile', 'tablet') 
			THEN 'mobile-web'
		WHEN NULLIF(d.device, '') IS NULL 
			THEN 'unknown'
		ELSE 'ERROR'
		END AS device_type
	, TIMESTAMP_ADD(d.created_stmp, INTERVAL d.created_seconds SECOND) as device_created_tstmp
	, TIMESTAMP_ADD(d.updated_stmp, INTERVAL d.updated_seconds SECOND) as device_updated_tstmp

from device_2 d 
