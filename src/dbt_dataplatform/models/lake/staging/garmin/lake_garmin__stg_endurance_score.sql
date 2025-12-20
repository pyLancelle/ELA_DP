SELECT
	userProfilePK,
	startDate,
	endDate,
	avg,
	max,
	TO_JSON_STRING(groupMap) AS groupMap,
	enduranceScoreDTO AS enduranceScoreDTO,
	data_type,
	TIMESTAMP(`_dp_inserted_at`) AS _dp_inserted_at,
	`_source_file`
FROM {{ source('garmin','endurance_score') }}
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY startDate ORDER BY TIMESTAMP(_dp_inserted_at) DESC) = 1
