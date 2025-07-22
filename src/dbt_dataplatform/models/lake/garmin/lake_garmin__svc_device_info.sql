{{ config(
    dataset=get_schema('lake'),
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='device_id'
) }}

-- Pure device info data extraction from staging_garmin_raw
-- Source: device_info data type from Garmin Connect API

SELECT
    -- Device identifiers
    JSON_EXTRACT_SCALAR(raw_data, '$.deviceId') AS device_id,
    JSON_EXTRACT_SCALAR(raw_data, '$.productDisplayName') AS product_display_name,
    JSON_EXTRACT_SCALAR(raw_data, '$.serialNumber') AS serial_number,
    JSON_EXTRACT_SCALAR(raw_data, '$.partNumber') AS part_number,
    
    -- Firmware and versions
    JSON_EXTRACT_SCALAR(raw_data, '$.currentFirmwareVersion') AS current_firmware_version,
    JSON_EXTRACT_SCALAR(raw_data, '$.softwareVersion') AS software_version,
    JSON_EXTRACT_SCALAR(raw_data, '$.gpsVersion') AS gps_version,
    
    -- Device classification
    JSON_EXTRACT_SCALAR(raw_data, '$.deviceCategory') AS device_category,
    JSON_EXTRACT_SCALAR(raw_data, '$.deviceTypePk') AS device_type_pk,
    JSON_EXTRACT_SCALAR(raw_data, '$.displayName') AS display_name,
    
    -- Status and dates
    JSON_EXTRACT_SCALAR(raw_data, '$.deviceStatus') AS device_status,
    SAFE.PARSE_DATE('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_data, '$.releaseDate')) AS release_date,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', JSON_EXTRACT_SCALAR(raw_data, '$.lastSyncTime')) AS last_sync_time,
    
    -- All capabilities preserved as JSON for hub layer processing
    -- (device_info has 100+ capability flags - too many to extract individually at lake level)
    raw_data AS device_capabilities_full_json,
    
    -- Source metadata
    dp_inserted_at,
    source_file

FROM {{ source('garmin', 'lake_garmin__stg_garmin_raw') }}
WHERE data_type = 'device_info'
  AND JSON_EXTRACT_SCALAR(raw_data, '$.deviceId') IS NOT NULL

{% if is_incremental() %}
  AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}