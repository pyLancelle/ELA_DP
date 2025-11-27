{{
  config(
      tags=['garmin', 'lake']
  )
}}

SELECT
	userId,
	mostRecentVO2Max,
	mostRecentTrainingLoadBalance,
	mostRecentTrainingStatus,
	`date`,
	data_type,
	`_dp_inserted_at`,
	`_source_file`
FROM {{ source('garmin','training_status') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY _dp_inserted_at DESC) = 1
