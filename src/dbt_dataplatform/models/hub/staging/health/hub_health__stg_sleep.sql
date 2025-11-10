/*
Dans cette table, on veut une ligne par "sleep".
Colonnes :
date
score de sommeil
heure de coucher
heure de lever
deep sleep :
- temps
- pourcentage de la nuit
- optimal min
- optimal max
light sleep :
- temps
- pourcentage de la nuit
- optimal min
- optimal max
REM sleep :
- temps
- pourcentage de la nuit
- optimal min
- optimal max

bodybattery :
- au moment du coucher
- au moment du lever
- delta

hrv : moyen pendant la nuit
stress : moyen pendant la nuit


*/

{{
  config(
      tags=['health', 'hub'],
      materialized='view'
  )
}}

SELECT
    -- Basic information
    DATE(calendardate) AS date,
    sleepscores.overallValue AS sleep_score,
    sleepstarttimestamplocal AS bedtime,
    sleependtimestamplocal AS waketime,
    sleeptimeseconds AS total_sleep_seconds,

    -- Deep Sleep (nested)
    STRUCT(
        deepsleepseconds,
        ROUND(100.0 * deepsleepseconds / NULLIF(sleeptimeseconds, 0), 2) AS pct,
        sleepscores.deepPercentageIdealStartInSeconds AS optimal_min_sec,
        sleepscores.deepPercentageIdealEndInSeconds AS optimal_max_sec
    ) AS deep_sleep,

    -- Light Sleep (nested)
    STRUCT(
        lightsleepseconds AS seconds,
        ROUND(100.0 * lightsleepseconds / NULLIF(sleeptimeseconds, 0), 2) AS pct,
        sleepscores.lightPercentageIdealStartInSeconds AS optimal_min_sec,
        sleepscores.lightPercentageIdealEndInSeconds AS optimal_max_sec
    ) AS light_sleep,

    -- REM Sleep (nested)
    STRUCT(
        remsleepseconds AS seconds,
        ROUND(100.0 * remsleepseconds / NULLIF(sleeptimeseconds, 0), 2) AS pct,
        sleepscores.remPercentageIdealStartInSeconds AS optimal_min_sec,
        sleepscores.remPercentageIdealEndInSeconds AS optimal_max_sec
    ) AS rem_sleep,

    -- Awake (nested)
    STRUCT(
        awakesleepseconds AS seconds,
        ROUND(100.0 * awakesleepseconds / NULLIF(sleeptimeseconds, 0), 2) AS pct,
        awakecount AS count
    ) AS awake,

    -- Body Battery (nested)
    STRUCT(
        CASE
            WHEN ARRAY_LENGTH(sleepbodybattery) > 0 THEN sleepbodybattery[OFFSET(0)].value
            ELSE NULL
        END AS bedtime,
        CASE
            WHEN ARRAY_LENGTH(sleepbodybattery) > 0 THEN sleepbodybattery[OFFSET(ARRAY_LENGTH(sleepbodybattery) - 1)].value
            ELSE NULL
        END AS waketime,
        bodybatterychange AS delta
    ) AS body_battery,

    -- Health metrics
    avgovernighthrv AS avg_hrv,
    avgsleepstress AS avg_stress

FROM
    {{ ref('lake_garmin__svc_sleep') }}
WHERE
    calendardate IS NOT NULL
ORDER BY
    calendardate DESC
