SELECT
	userProfilePk,
	hrvSummary,
	hrvReadings,
	startTimestampGMT,
	endTimestampGMT,
	startTimestampLocal,
	endTimestampLocal,
	`date`,
	data_type,
	TIMESTAMP(`_dp_inserted_at`) AS _dp_inserted_at,
	`_source_file`,
	sleepStartTimestampGMT,
	sleepEndTimestampGMT,
	sleepStartTimestampLocal,
	sleepEndTimestampLocal
FROM {{ source('garmin','hrv') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY TIMESTAMP(_dp_inserted_at) DESC) = 1