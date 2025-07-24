{{ config(dataset=get_schema('lake')) }}

-- Spotify staging raw data with universal JSON schema
-- Similar to Garmin's approach for consistent architecture
SELECT *
FROM {{ source('spotify', 'lake_spotify__stg_spotify_raw') }}
