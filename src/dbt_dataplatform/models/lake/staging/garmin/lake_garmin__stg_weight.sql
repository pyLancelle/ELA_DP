SELECT
	samplePk,
	`date`,
	calendarDate,
	weight,
	bodyFat,
	bodyWater,
	boneMass,
	muscleMass,
	sourceType,
	timestampGMT,
	weightDelta,
	summaryDate,
	data_type,
	TIMESTAMP(`_dp_inserted_at`) AS _dp_inserted_at,
	`_source_file`,
	bmi
FROM {{ source('garmin','weight') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY calendarDate ORDER BY TIMESTAMP(_dp_inserted_at) DESC) = 1
