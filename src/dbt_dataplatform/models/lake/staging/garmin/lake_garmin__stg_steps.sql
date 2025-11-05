SELECT
    STARTGMT,
    ENDGMT,
    `date`,
    STEPS,
    PUSHES,
    PRIMARYACTIVITYLEVEL,
    ACTIVITYLEVELCONSTANT,
    DP_INSERTED_AT
FROM {{ source('garmin','steps') }}
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY startgmt ORDER BY dp_inserted_at DESC) = 1
