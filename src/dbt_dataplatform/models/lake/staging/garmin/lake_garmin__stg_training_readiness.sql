{{
  config(
      tags=['garmin', 'lake']
  )
}}

SELECT
	userProfilePK,
	calendarDate,
	`timestamp`,
	timestampLocal,
	deviceId,
	`level`,
	feedbackLong,
	feedbackShort,
	sleepScoreFactorPercent,
	sleepScoreFactorFeedback,
	recoveryTime,
	recoveryTimeFactorPercent,
	recoveryTimeFactorFeedback,
	acwrFactorPercent,
	acwrFactorFeedback,
	stressHistoryFactorPercent,
	stressHistoryFactorFeedback,
	hrvFactorPercent,
	hrvFactorFeedback,
	sleepHistoryFactorPercent,
	sleepHistoryFactorFeedback,
	validSleep,
	primaryActivityTracker,
	`date`,
	data_type,
	`_dp_inserted_at`,
	`_source_file`,
	sleepScore,
	recoveryTimeChangePhrase,
	score,
	hrvWeeklyAverage,
	acuteLoad,
	inputContext,
FROM {{ source('garmin','training_readiness') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY calendarDate, timestamp ORDER BY _dp_inserted_at DESC) = 1
