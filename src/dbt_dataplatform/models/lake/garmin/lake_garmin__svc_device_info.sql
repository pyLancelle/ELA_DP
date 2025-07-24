{{ config(
    dataset=get_schema('lake'),
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='device_id',
    tags=["lake", "garmin"]
) }}

-- Pure Lake model for Garmin device info data
-- Stores raw JSON data with basic metadata and deduplication only
-- All field extraction logic will be moved to Hub layer
-- Note: Original model had no ROW_NUMBER logic, using simple unique_key approach

SELECT
    -- Unique identifier for deduplication
    JSON_EXTRACT_SCALAR(raw_data, '$.deviceId') AS device_id,
    
    -- Complete raw JSON data (to be parsed in Hub layer)
    raw_data,
    
    -- Data type for consistency
    data_type,
    
    -- Source metadata
    dp_inserted_at,
    source_file

FROM {{ source('garmin', 'lake_garmin__stg_garmin_raw') }}
WHERE data_type = 'device_info'
  AND JSON_EXTRACT_SCALAR(raw_data, '$.deviceId') IS NOT NULL

{% if is_incremental() %}
  AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}