SELECT
	userProfilePK,
	startDate,
	endDate,
	maxScore,
	periodAvgScore,
	dailyScores,
	dp_inserted_at
FROM {{ source('garmin','hill_score') }}