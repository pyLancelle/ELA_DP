SELECT
	activityId,
	activityName,
	startTimeLocal,
	data_type,
	activityType,
	weatherData,
	dp_inserted_at,
FROM {{ source('garmin','activity_weather') }}