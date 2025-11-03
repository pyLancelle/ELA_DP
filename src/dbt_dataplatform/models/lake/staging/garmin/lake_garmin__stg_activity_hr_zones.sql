SELECT
	activityId,
	activityName,
	startTimeLocal,
	data_type,
	activityType,
	hrZonesData,
	dp_inserted_at
FROM {{ source('garmin','activity_hr_zones') }}