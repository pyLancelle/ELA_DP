{{
  config(
      tags=['garmin', 'lake']
  )
}}

SELECT
    STARTTIMESTAMPGMT,
    ENDTIMESTAMPGMT,
    STARTTIMESTAMPLOCAL,
    ENDTIMESTAMPLOCAL,
    CALENDARDATE,
    FLOORINTERVALS,
    DP_INSERTED_AT
FROM {{ source('garmin','floors') }}
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY calendardate ORDER BY dp_inserted_at DESC)
    = 1
