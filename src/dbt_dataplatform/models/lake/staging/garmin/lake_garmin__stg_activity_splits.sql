SELECT
	activityId,
	activityName,
	startTimeLocal,
	data_type,
	activityType,
	splitSummaries,
	dp_inserted_at
FROM {{ source('garmin','activity_splits') }}