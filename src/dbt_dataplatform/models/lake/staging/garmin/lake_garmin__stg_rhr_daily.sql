SELECT
	userProfileId,
	statisticsStartDate,
	statisticsEndDate,
	allMetrics,
	`date`,
	data_type,
	`_dp_inserted_at`,
	`_source_file`
FROM {{ source('garmin','rhr_daily') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY _dp_inserted_at DESC) = 1
