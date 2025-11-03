SELECT
    userProfilePK,
    calendarDate,
    startTimestampGMT,
    endTimestampGMT,
    startTimestampLocal,
    endTimestampLocal,
    maxStressLevel,
    avgStressLevel,
    stressChartValueOffset,
    stressChartYAxisOrigin,
    stressValues,
    bodyBatteryValues,
    dp_inserted_at
FROM {{ source('garmin','stress') }}