SELECT
	userProfileId,
	statisticsStartDate,
	statisticsEndDate,
	allMetrics,
	`date`,
	data_type,
	TIMESTAMP(`_dp_inserted_at`) AS _dp_inserted_at,
	`_source_file`
FROM {{ source('garmin','rhr_daily') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY TIMESTAMP(_dp_inserted_at) DESC) = 1
