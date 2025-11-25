{{
  config(
      tags=['garmin', 'lake']
  )
}}

SELECT
    `date`,
    charged,
    drained,
    startTimestampGMT,
    endTimestampGMT,
    startTimestampLocal,
    endTimestampLocal,
    bodyBatteryValuesArray,
    bodyBatteryValueDescriptorDTOList,
    bodyBatteryDynamicFeedbackEvent,
    bodyBatteryActivityEvent,
    endOfDayBodyBatteryDynamicFeedbackEvent,
    data_type,
    `_dp_inserted_at`,
    `_source_file`,
FROM {{ source('garmin','body_battery') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY _dp_inserted_at DESC) = 1
