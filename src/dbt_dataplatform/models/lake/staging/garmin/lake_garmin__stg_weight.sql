SELECT
    SAMPLEPK,
    `date`,
    CALENDARDATE,
    WEIGHT,
    BMI,
    BODYFAT,
    BODYWATER,
    BONEMASS,
    MUSCLEMASS,
    PHYSIQUERATING,
    VISCERALFAT,
    METABOLICAGE,
    SOURCETYPE,
    TIMESTAMPGMT,
    WEIGHTDELTA,
    SUMMARYDATE,
    DP_INSERTED_AT
FROM {{ source('garmin','weight') }}
QUALIFY
    ROW_NUMBER()
        OVER (
            PARTITION BY CALENDARDATE
            ORDER BY dp_inserted_at DESC
        )
    = 1
