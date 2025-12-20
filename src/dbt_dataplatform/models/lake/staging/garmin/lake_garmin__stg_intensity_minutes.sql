SELECT
	userProfilePK,
	calendarDate,
	weekGoal,
	`date`,
	data_type,
	TIMESTAMP(`_dp_inserted_at`) AS _dp_inserted_at,
	`_source_file`,
	weeklyModerate,
	weeklyVigorous,
	weeklyTotal,
	dayOfGoalMet,
	startDayMinutes,
	endDayMinutes,
	startTimestampGMT,
	endTimestampGMT,
	startTimestampLocal,
	endTimestampLocal,
	moderateMinutes,
	vigorousMinutes,
	imValueDescriptorsDTOList,
	imValuesArray
FROM {{ source('garmin','intensity_minutes') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY calendarDate ORDER BY TIMESTAMP(_dp_inserted_at) DESC) = 1