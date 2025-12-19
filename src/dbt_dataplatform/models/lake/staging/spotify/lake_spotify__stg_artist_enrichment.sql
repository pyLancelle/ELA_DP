{{
  config(
      tags=['spotify', 'lake', 'artist_enrichment']
  )
}}

SELECT
    -- Core artist identification
    artistId,
    artistName,
    uri,
    href,

    -- Metrics
    popularity,
    followerCount,

    -- Genres: Keep JSON for flexibility + extract as array for easy querying
    JSON_VALUE_ARRAY(GENRES) AS genres,
    
    -- Images
    STRUCT(
        JSON_EXTRACT_SCALAR(IMAGES, '$[0].url') AS url,
        CAST(JSON_EXTRACT_SCALAR(IMAGES, '$[0].height') AS INT64) AS height,
        CAST(JSON_EXTRACT_SCALAR(IMAGES, '$[0].width') AS INT64) AS width
    ) AS image_large,
    STRUCT(
        JSON_EXTRACT_SCALAR(IMAGES, '$[1].url') AS url,
        CAST(JSON_EXTRACT_SCALAR(IMAGES, '$[1].height') AS INT64) AS height,
        CAST(JSON_EXTRACT_SCALAR(IMAGES, '$[1].width') AS INT64) AS width
    ) AS image_medium,
    STRUCT(
        JSON_EXTRACT_SCALAR(IMAGES, '$[2].url') AS url,
        CAST(JSON_EXTRACT_SCALAR(IMAGES, '$[2].height') AS INT64) AS height,
        CAST(JSON_EXTRACT_SCALAR(IMAGES, '$[2].width') AS INT64) AS width
    ) AS image_small,

    -- Quick access to image URLs (most commonly used)
    JSON_EXTRACT_SCALAR(IMAGES, '$[0].url') AS imageUrlLarge,
    JSON_EXTRACT_SCALAR(IMAGES, '$[1].url') AS imageUrlMedium,
    JSON_EXTRACT_SCALAR(IMAGES, '$[2].url') AS imageUrlSmall,

    -- Metadata
    enrichedAt,
    dp_inserted_at AS _dp_inserted_at,
FROM {{ source('spotify','artist_enrichment') }}
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY artistId ORDER BY enrichedAt DESC, _dp_inserted_at DESC) = 1
