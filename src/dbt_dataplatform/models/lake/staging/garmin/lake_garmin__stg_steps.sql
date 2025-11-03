SELECT
	startGMT,
	endGMT,
	`date`,
	steps,
	pushes,
	primaryActivityLevel,
	activityLevelConstant,
	dp_inserted_at
FROM {{ source('garmin','steps') }}