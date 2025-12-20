SELECT
	userId,
	mostRecentVO2Max,
	mostRecentTrainingLoadBalance,
	mostRecentTrainingStatus,
	`date`,
	data_type,
	TIMESTAMP(`_dp_inserted_at`) AS _dp_inserted_at,
	`_source_file`
FROM {{ source('garmin','training_status') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY TIMESTAMP(_dp_inserted_at) DESC) = 1
