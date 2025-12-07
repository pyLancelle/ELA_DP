SELECT
	userProfilePK,
	calendarDate,
	startTimestampGMT,
	endTimestampGMT,
	startTimestampLocal,
	endTimestampLocal,
	maxStressLevel,
	avgStressLevel,
	stressChartValueOffset,
	stressChartYAxisOrigin,
	stressValueDescriptorsDTOList,
	stressValuesArray,
	`date`,
	data_type,
	`_dp_inserted_at`,
	`_source_file`,
	bodyBatteryValueDescriptorsDTOList,
	bodyBatteryValuesArray
FROM {{ source('garmin','stress') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY calendarDate ORDER BY _dp_inserted_at DESC) = 1
