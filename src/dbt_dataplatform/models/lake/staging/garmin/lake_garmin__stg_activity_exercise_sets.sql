SELECT
	activityId,
	activityName,
	activityType,
	startTimeLocal,
	activity_exercise_sets_data,
	data_type,
	exercise_sets_data,
	TIMESTAMP(`_dp_inserted_at`) AS _dp_inserted_at,
	`_source_file`
FROM {{ source('garmin','activity_exercise_sets') }}
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY activityid ORDER BY TIMESTAMP(_dp_inserted_at) DESC) = 1
