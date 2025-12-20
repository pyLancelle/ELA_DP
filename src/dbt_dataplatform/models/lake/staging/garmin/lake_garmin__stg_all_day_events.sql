SELECT
	userProfilePk,
	deviceId,
	calendarDate,
	startTimestampGMT,
	endTimestampGMT,
	duration,
	activityType,
	startTimestampLocal,
	endTimestampLocal,
	`date`,
	data_type,
	TIMESTAMP(`_dp_inserted_at`) AS _dp_inserted_at,
	`_source_file`
FROM {{ source('garmin','all_day_events') }}
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY calendarDate, startTimestampLocal ORDER BY TIMESTAMP(_dp_inserted_at) DESC) = 1
