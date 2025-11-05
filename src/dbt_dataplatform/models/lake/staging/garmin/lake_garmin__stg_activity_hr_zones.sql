SELECT
    ACTIVITYID,
    ACTIVITYNAME,
    STARTTIMELOCAL,
    DATA_TYPE,
    ACTIVITYTYPE,
    HRZONESDATA,
    DP_INSERTED_AT
FROM {{ source('garmin','activity_hr_zones') }}
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY activityid ORDER BY dp_inserted_at DESC) = 1
