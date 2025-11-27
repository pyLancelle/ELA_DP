{{
  config(
      tags=['garmin', 'lake']
  )
}}

SELECT
	userProfilePK,
	calendarDate,
	respirationVersion,
	`date`,
	data_type,
	`_dp_inserted_at`,
	`_source_file`,
	startTimestampGMT,
	endTimestampGMT,
	startTimestampLocal,
	endTimestampLocal,
	tomorrowSleepStartTimestampGMT,
	tomorrowSleepEndTimestampGMT,
	tomorrowSleepStartTimestampLocal,
	tomorrowSleepEndTimestampLocal,
	lowestRespirationValue,
	highestRespirationValue,
	avgWakingRespirationValue,
	avgTomorrowSleepRespirationValue,
	respirationValueDescriptorsDTOList,
	respirationValuesArray,
	respirationAveragesValueDescriptorDTOList,
	respirationAveragesValuesArray,
	sleepStartTimestampGMT,
	sleepEndTimestampGMT,
	sleepStartTimestampLocal,
	sleepEndTimestampLocal,
	avgSleepRespirationValue
FROM {{ source('garmin','respiration') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY calendarDate ORDER BY _dp_inserted_at DESC) = 1
