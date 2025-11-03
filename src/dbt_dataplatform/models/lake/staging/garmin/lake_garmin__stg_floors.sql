SELECT
	startTimestampGMT,
	endTimestampGMT,
	startTimestampLocal,
	endTimestampLocal,
	calendarDate,
	floorIntervals,
	dp_inserted_at
FROM {{ source('garmin','floors') }}