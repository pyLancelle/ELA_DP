SELECT
	startGMT,
	endGMT,
	steps,
	pushes,
	primaryActivityLevel,
	activityLevelConstant,
	`date`,
	data_type,
	`_dp_inserted_at`,
	`_source_file`
FROM {{ source('garmin','steps') }}
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY startGMT ORDER BY _dp_inserted_at DESC) = 1
