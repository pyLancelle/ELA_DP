{{ config(dataset=get_schema('hub'), materialized='view', tags=["hub", "spotify"]) }}

-- Hub model for Spotify saved tracks with structured objects
-- Uses STRUCT to preserve logical groupings similar to Garmin's approach

SELECT
    -- Core identifiers and timing (kept flat for easy filtering/joining)
    saved_track_id,
    TIMESTAMP(JSON_VALUE(raw_data, '$.added_at')) as added_at,
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
        CAST(JSON_VALUE(raw_data, '$.track.is_playable') AS BOOL) as is_playable,
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
        CAST(JSON_VALUE(raw_data, '$.track.album.is_playable') AS BOOL) as is_playable,
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
    
    -- Metadata
    dp_inserted_at,
    source_file

FROM {{ ref('lake_spotify__svc_saved_tracks') }}
WHERE JSON_VALUE(raw_data, '$.added_at') IS NOT NULL
  AND JSON_VALUE(raw_data, '$.track.id') IS NOT NULL