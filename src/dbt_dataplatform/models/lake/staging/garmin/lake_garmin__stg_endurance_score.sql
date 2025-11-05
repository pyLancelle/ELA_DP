{{
  config(
      tags=['garmin', 'lake']
  )
}}

SELECT
    USERPROFILEPK,
    STARTDATE,
    ENDDATE,
    AVG,
    MAX,
    ENDURANCESCOREDTO,
    CONTRIBUTORS,
    DP_INSERTED_AT
FROM {{ source('garmin','endurance_score') }}
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY startdate ORDER BY dp_inserted_at DESC) = 1
