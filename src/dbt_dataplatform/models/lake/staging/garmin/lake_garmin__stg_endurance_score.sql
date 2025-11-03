SELECT
	userProfilePK,
	startDate,
	endDate,
	avg,
	max,
	enduranceScoreDTO,
	contributors,
	dp_inserted_at,
FROM {{ source('garmin','endurance_score') }}