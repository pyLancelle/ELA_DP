{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "spotify"]) }}

-- Hub model for Spotify recently played tracks with structured objects
-- Uses STRUCT to preserve logical groupings similar to Garmin's approach

WITH base AS (
    SELECT
        -- Core identifiers and timing (kept flat for easy filtering/joining)
        play_id,
        TIMESTAMP(JSON_VALUE(raw_data, '$.played_at')) as played_at,
        JSON_VALUE(raw_data, '$.track.id') as track_id,
        JSON_VALUE(raw_data, '$.track.name') as track_name,
        
        -- Track information as STRUCT
        STRUCT(
            JSON_VALUE(raw_data, '$.track.id') as id,
            JSON_VALUE(raw_data, '$.track.name') as name,
            CAST(JSON_VALUE(raw_data, '$.track.duration_ms') AS INT64) as duration_ms,
            CAST(JSON_VALUE(raw_data, '$.track.explicit') AS BOOL) as explicit,
            CAST(JSON_VALUE(raw_data, '$.track.popularity') AS INT64) as popularity,
            CAST(JSON_VALUE(raw_data, '$.track.disc_number') AS INT64) as disc_number,
            CAST(JSON_VALUE(raw_data, '$.track.track_number') AS INT64) as track_number,
            CAST(JSON_VALUE(raw_data, '$.track.is_local') AS BOOL) as is_local,
            JSON_VALUE(raw_data, '$.track.preview_url') as preview_url,
            JSON_VALUE(raw_data, '$.track.type') as type,
            JSON_VALUE(raw_data, '$.track.uri') as uri,
            JSON_QUERY_ARRAY(raw_data, '$.track.available_markets') as available_markets,
            
            -- External URLs and IDs nested
            STRUCT(
                JSON_VALUE(raw_data, '$.track.external_urls.spotify') as spotify
            ) as external_urls,
            
            STRUCT(
                JSON_VALUE(raw_data, '$.track.external_ids.isrc') as isrc
            ) as external_ids,
            
            JSON_VALUE(raw_data, '$.track.href') as href
        ) as track,
        
        -- Album information as STRUCT
        STRUCT(
            JSON_VALUE(raw_data, '$.track.album.id') as id,
            JSON_VALUE(raw_data, '$.track.album.name') as name,
            JSON_VALUE(raw_data, '$.track.album.album_type') as album_type,
            JSON_VALUE(raw_data, '$.track.album.release_date') as release_date,
            JSON_VALUE(raw_data, '$.track.album.release_date_precision') as release_date_precision,
            CAST(JSON_VALUE(raw_data, '$.track.album.total_tracks') AS INT64) as total_tracks,
            JSON_VALUE(raw_data, '$.track.album.type') as type,
            JSON_VALUE(raw_data, '$.track.album.uri') as uri,
            JSON_QUERY_ARRAY(raw_data, '$.track.album.available_markets') as available_markets,
            
            -- Album external URLs
            STRUCT(
                JSON_VALUE(raw_data, '$.track.album.external_urls.spotify') as spotify
            ) as external_urls,
            
            JSON_VALUE(raw_data, '$.track.album.href') as href,
            
            -- Album images as array of STRUCT
            ARRAY(
                SELECT AS STRUCT
                    CAST(JSON_VALUE(image, '$.height') AS INT64) as height,
                    CAST(JSON_VALUE(image, '$.width') AS INT64) as width,
                    JSON_VALUE(image, '$.url') as url
                FROM UNNEST(JSON_QUERY_ARRAY(raw_data, '$.track.album.images')) as image
            ) as images,
            
            -- Album artists as array of STRUCT
            ARRAY(
                SELECT AS STRUCT
                    JSON_VALUE(artist, '$.id') as id,
                    JSON_VALUE(artist, '$.name') as name,
                    JSON_VALUE(artist, '$.type') as type,
                    JSON_VALUE(artist, '$.uri') as uri,
                    JSON_VALUE(artist, '$.href') as href,
                    STRUCT(
                        JSON_VALUE(artist, '$.external_urls.spotify') as spotify
                    ) as external_urls
                FROM UNNEST(JSON_QUERY_ARRAY(raw_data, '$.track.album.artists')) as artist
            ) as artists
        ) as album,
        
        -- Track artists as array of STRUCT
        ARRAY(
            SELECT AS STRUCT
                JSON_VALUE(artist, '$.id') as id,
                JSON_VALUE(artist, '$.name') as name,
                JSON_VALUE(artist, '$.type') as type,
                JSON_VALUE(artist, '$.uri') as uri,
                JSON_VALUE(artist, '$.href') as href,
                STRUCT(
                    JSON_VALUE(artist, '$.external_urls.spotify') as spotify
                ) as external_urls
            FROM UNNEST(JSON_QUERY_ARRAY(raw_data, '$.track.artists')) as artist
        ) as artists,
        
        -- Context information as STRUCT
        STRUCT(
            JSON_VALUE(raw_data, '$.context.type') as type,
            JSON_VALUE(raw_data, '$.context.uri') as uri,
            JSON_VALUE(raw_data, '$.context.href') as href,
            STRUCT(
                JSON_VALUE(raw_data, '$.context.external_urls.spotify') as spotify
            ) as external_urls
        ) as context,
        
        -- Metadata
        dp_inserted_at,
        source_file
    FROM {{ ref('lake_spotify__svc_recently_played') }}
    WHERE JSON_VALUE(raw_data, '$.played_at') IS NOT NULL
      AND JSON_VALUE(raw_data, '$.track.id') IS NOT NULL
),
ordered AS (
    SELECT
        base.*,
        LEAD(played_at) OVER (ORDER BY played_at, play_id) AS next_played_at
    FROM base
),
with_expected_end AS (
    SELECT
        ordered.*,
        TIMESTAMP_ADD(
            played_at,
            INTERVAL COALESCE(track.duration_ms, 0) MILLISECOND
        ) AS expected_end_at
    FROM ordered
),
with_actual_end AS (
    SELECT
        with_expected_end.*,
        CASE
            WHEN next_played_at IS NULL THEN expected_end_at
            ELSE LEAST(expected_end_at, next_played_at)
        END AS actual_end_at
    FROM with_expected_end
),
final AS (
    SELECT
        with_actual_end.*,
        GREATEST(
            0,
            TIMESTAMP_DIFF(actual_end_at, played_at, MILLISECOND)
        ) AS actual_duration_ms
    FROM with_actual_end
)
SELECT
    play_id,
    played_at,
    track_id,
    track_name,
    expected_end_at,
    next_played_at,
    actual_end_at,
    actual_duration_ms,
    SAFE_DIVIDE(actual_duration_ms, 1000.0) AS actual_duration_seconds,
    track,
    album,
    artists,
    context,
    dp_inserted_at,
    source_file
FROM final
ORDER BY played_at, play_id
