{{
  config(
      tags=['garmin', 'lake']
  )
}}

SELECT
	userProfilePK,
	calendarDate,
	startTimestampGMT,
	endTimestampGMT,
	startTimestampLocal,
	endTimestampLocal,
	tomorrowSleepStartTimestampGMT,
	tomorrowSleepEndTimestampGMT,
	tomorrowSleepStartTimestampLocal,
	tomorrowSleepEndTimestampLocal,
	`date`,
	data_type,
	`_dp_inserted_at`,
	`_source_file`,
	sleepStartTimestampGMT,
	sleepEndTimestampGMT,
	sleepStartTimestampLocal,
	sleepEndTimestampLocal,
	averageSpO2,
	lowestSpO2,
	lastSevenDaysAvgSpO2,
	latestSpO2,
	latestSpO2TimestampGMT,
	latestSpO2TimestampLocal,
	avgTomorrowSleepSpO2,
	avgSleepSpO2,
	spO2ValueDescriptorsDTOList,
	spO2HourlyAverages,
	spO2SingleValues,
FROM {{ source('garmin','spo2') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY calendardate ORDER BY _dp_inserted_at DESC) = 1
