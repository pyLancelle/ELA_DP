SELECT * EXCEPT(polyline, timeseries)
FROM {{ ref('hub_health__svc_activities') }}
ORDER BY activityId DESC
LIMIT 3