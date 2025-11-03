SELECT
	`date`,
	startTimestampGMT,
	endTimestampGMT,
	startTimestampLocal,
	endTimestampLocal,
	charged,
	drained,
	bodyBatteryValues,
	bodyBatteryDynamicFeedbackEvent,
	endOfDayBodyBatteryDynamicFeedbackEvent,
	bodyBatteryActivityEvent,
	dp_inserted_at
FROM {{ source('garmin','body_battery') }}