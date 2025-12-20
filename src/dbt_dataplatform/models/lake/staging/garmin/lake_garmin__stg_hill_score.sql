SELECT
	userProfilePK,
	startDate,
	endDate,
	TO_JSON_STRING(periodAvgScore) AS periodAvgScore,
	maxScore,
	hillScoreDTOList,
	data_type,
	TIMESTAMP(`_dp_inserted_at`) AS _dp_inserted_at,
	`_source_file`
FROM {{ source('garmin','hill_score') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY startDate ORDER BY enddate DESC, _dp_inserted_at DESC) = 1

