SELECT *
FROM {{ source('spotify', 'staging_spotify_raw') }}
