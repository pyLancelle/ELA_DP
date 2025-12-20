SELECT
	activityId,
	activityName,
	activityType,
	startTimeLocal,
	activity_weather_data,
	data_type,
	weather_data,
	TIMESTAMP(`_dp_inserted_at`) AS _dp_inserted_at,
	`_source_file`
FROM {{ source('garmin','activity_weather') }}
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY activityid ORDER BY TIMESTAMP(_dp_inserted_at) DESC) = 1
