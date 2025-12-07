SELECT
	activityId,
	activityName,
	activityType,
	startTimeLocal,
	activity_hr_zones_data,
	data_type,
	hr_zones_data,
	`_dp_inserted_at`,
	`_source_file`
FROM {{ source('garmin','activity_hr_zones') }}
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY activityid ORDER BY _dp_inserted_at DESC) = 1
