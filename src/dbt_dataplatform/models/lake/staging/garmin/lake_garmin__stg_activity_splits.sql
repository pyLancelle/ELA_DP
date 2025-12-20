SELECT
	activityId,
	activityName,
	activityType,
	startTimeLocal,
	splits,
	typed_splits,
	split_summaries,
	data_type,
	TIMESTAMP(`_dp_inserted_at`) AS _dp_inserted_at,
	`_source_file`
FROM {{ source('garmin','activity_splits') }}
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY activityid ORDER BY TIMESTAMP(_dp_inserted_at) DESC) = 1
