SELECT
	userId,
	generic,
	heatAltitudeAcclimation,
	`date`,
	data_type,
	`_dp_inserted_at`,
	`_source_file`
FROM {{ source('garmin','max_metrics') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY _dp_inserted_at DESC) = 1