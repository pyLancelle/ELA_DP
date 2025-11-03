SELECT *
FROM {{ source('garmin','normalized_race_predictions')}}