{{ config(
    dataset=get_schema('lake'),
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['floors_date', 'day_start_gmt']
) }}

-- Pure floors climbed data extraction from staging_garmin_raw
-- Source: floors data type from Garmin Connect API
-- Stores 15-minute interval data as time-series arrays
-- Deduplicates by keeping most recent record per date/timestamp combination

WITH floors_data_with_rank AS (
  SELECT
    -- Date and day timestamps
    DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.date'))) AS floors_date,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.startTimestampGMT')) AS day_start_gmt,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.endTimestampGMT')) AS day_end_gmt,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.startTimestampLocal')) AS day_start_local,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.endTimestampLocal')) AS day_end_local,
    
    -- Floor climbing data stored as arrays - preserved for hub layer processing
    -- Array format: [startTime, endTime, floorsAscended, floorsDescended]
    JSON_EXTRACT(raw_data, '$.floorValuesArray') AS floor_values_timeseries_json,
    JSON_EXTRACT(raw_data, '$.floorsValueDescriptorDTOList') AS timeseries_descriptor_json,
    
    -- Source metadata
    dp_inserted_at,
    source_file,
    
    -- Add row number to deduplicate by most recent dp_inserted_at
    ROW_NUMBER() OVER (
      PARTITION BY 
        DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.date'))),
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E1S', JSON_EXTRACT_SCALAR(raw_data, '$.startTimestampGMT'))
      ORDER BY dp_inserted_at DESC
    ) AS row_rank

  FROM {{ source('garmin', 'lake_garmin__stg_garmin_raw') }}
  WHERE data_type = 'floors'
    AND JSON_EXTRACT_SCALAR(raw_data, '$.date') IS NOT NULL
    
  {% if is_incremental() %}
    AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
  {% endif %}
)

SELECT
  floors_date,
  day_start_gmt,
  day_end_gmt,
  day_start_local,
  day_end_local,
  floor_values_timeseries_json,
  timeseries_descriptor_json,
  dp_inserted_at,
  source_file

FROM floors_data_with_rank
WHERE row_rank = 1