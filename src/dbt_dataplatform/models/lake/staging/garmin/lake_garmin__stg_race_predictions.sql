SELECT
	userId,
	calendarDate,
	time5K,
	time10K,
	timeHalfMarathon,
	timeMarathon,
	data_type,
	TIMESTAMP(`_dp_inserted_at`) AS _dp_inserted_at,
	`_source_file`
FROM {{ source('garmin','race_predictions') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY calendardate ORDER BY TIMESTAMP(_dp_inserted_at) DESC) = 1
