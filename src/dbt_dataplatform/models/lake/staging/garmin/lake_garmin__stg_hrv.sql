SELECT
	userProfilePk,
	`date`,
	startTimestampGMT,
	endTimestampGMT,
	startTimestampLocal,
	endTimestampLocal,
	sleepStartTimestampGMT,
	sleepEndTimestampGMT,
	sleepStartTimestampLocal,
	sleepEndTimestampLocal,
	hrvSummary,
	hrvReadings,
	dp_inserted_at
FROM {{ source('garmin','hrv') }}