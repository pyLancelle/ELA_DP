{{
  config(
      tags=['garmin', 'lake']
  )
}}

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
	`_dp_inserted_at`,
	`_source_file`
FROM {{ source('garmin','activity_weather') }}
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY calendarDate, startTimestampLocal ORDER BY _dp_inserted_at DESC) = 1
