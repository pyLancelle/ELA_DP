{{
  config(
      tags=['garmin', 'lake']
  )
}}

SELECT
    USERPROFILEPK,
    STARTDATE,
    ENDDATE,
    MAXSCORE,
    PERIODAVGSCORE,
    DAILYSCORES,
    DP_INSERTED_AT
FROM {{ source('garmin','hill_score') }}
QUALIFY
    ROW_NUMBER()
        OVER (PARTITION BY startdate ORDER BY enddate DESC, dp_inserted_at DESC)
    = 1

