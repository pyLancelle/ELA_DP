{{
  config(
      tags=['garmin', 'lake']
  )
}}

SELECT
	dailySleepDTO,
	remSleepData,
	skinTempDataExists,
	bodyBatteryChange,
	restingHeartRate,
	`date`,
	data_type,
	`_dp_inserted_at`,
	`_source_file`,
	wellnessSpO2SleepSummaryDTO,
	sleepMovement,
	sleepLevels,
	sleepRestlessMoments,
	restlessMomentsCount,
	wellnessEpochSPO2DataDTOList,
	wellnessEpochRespirationDataDTOList,
	wellnessEpochRespirationAveragesList,
	respirationVersion,
	sleepHeartRate,
	sleepStress,
	sleepBodyBattery,
	hrvData,
	breathingDisruptionData,
	avgOvernightHrv,
	hrvStatus,
FROM {{ source('garmin','sleep') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY calendardate ORDER BY _dp_inserted_at DESC) = 1
