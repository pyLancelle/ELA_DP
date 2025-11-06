SELECT
    activityname,
    starttimelocal,
    round(distance / 1000.0, 1) as distance,
    {{ seconds_to_hms('duration') }} as duration,
    {{ ms_to_min_per_km('averagespeed') }} as speed,
    round(1000 / (averagespeed * 60), 2) as pace_numeric, -- pour trier
FROM {{ ref('lake_garmin__svc_activities') }}
WHERE activitytype.typekey = 'running'
AND averagespeed > 0