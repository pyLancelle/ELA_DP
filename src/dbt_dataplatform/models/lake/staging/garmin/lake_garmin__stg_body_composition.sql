SELECT
    samplePk,
    `date`,
    calendarDate,
    weight,
    bodyFat,
    bodyWater,
    boneMass,
    muscleMass,
    bmi,
    sourceType,
    timestampGMT,
    data_type,
    `_dp_inserted_at`,
    `_source_file`
FROM {{ source('garmin','body_composition') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY _dp_inserted_at DESC) = 1
