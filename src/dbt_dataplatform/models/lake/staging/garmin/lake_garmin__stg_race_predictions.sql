SELECT
	userId,
	calendarDate,
	time5K,
	time10K,
	timeHalfMarathon,
	timeMarathon,
	data_type,
	`_dp_inserted_at`,
	`_source_file`
FROM {{ source('garmin','race_predictions') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY calendardate ORDER BY _dp_inserted_at DESC) = 1
