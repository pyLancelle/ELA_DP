SELECT
	userProfilePK,
	calendarDate,
	startTimestampGMT,
	endTimestampGMT,
	startTimestampLocal,
	endTimestampLocal,
	restingHeartRate,
	lastSevenDaysAvgRestingHeartRate,
	`date`,
	data_type,
	`_dp_inserted_at`,
	`_source_file`,
	maxHeartRate,
	minHeartRate,
	heartRateValues,
	heartRateValueDescriptors,
	abnormalHrThresholdValue,
	abnormalHRValuesArray
FROM {{ source('garmin','heart_rate') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY calendarDate ORDER BY _dp_inserted_at DESC) = 1
