SELECT
    userId,
    calendarDate,
    fromCalendarDate,
    toCalendarDate,
    time5K,
    time10K,
    timeHalfMarathon,
    timeMarathon,
    dp_inserted_at
FROM {{ source('garmin','race_predictions')}}