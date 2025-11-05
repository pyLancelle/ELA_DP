SELECT
    ACTIVITYID,
    ACTIVITYNAME,
    STARTTIMELOCAL,
    DATA_TYPE,
    ACTIVITYTYPE,
    WEATHERDATA,
    DP_INSERTED_AT
FROM {{ source('garmin','activity_weather') }}
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY activityid ORDER BY dp_inserted_at DESC) = 1
