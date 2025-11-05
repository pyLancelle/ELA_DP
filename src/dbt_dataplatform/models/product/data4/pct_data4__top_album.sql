WITH ranking AS (
    SELECT
        dim_albums.albumid,
        dim_albums.albumname,
        dim_albums.all_artist_names,
        sum(dim_tracks.trackDURATIONMS) AS total_duration_ms,
        count(distinct fact_played.playedat) AS play_count,
        dim_albums.albumexternalurl,
        dim_albums.albumimageurl
    FROM {{ ref('hub_music__svc_fact_played')}} AS fact_played
    LEFT JOIN {{ ref('hub_music__svc_dim_tracks')}} AS dim_tracks
        ON fact_played.trackid = dim_tracks.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_albums')}} AS dim_albums
        ON dim_tracks.albumid = dim_albums.albumid
    GROUP BY ALL
    ORDER BY total_duration_ms DESC
    LIMIT 20
)
SELECT
    ROW_NUMBER() OVER(ORDER BY total_duration_ms DESC) as rank,
    albumname,
    all_artist_names,
    {{ ms_to_hms('total_duration_ms') }} AS total_duration,
    play_count,
    albumexternalurl,
    albumimageurl
FROM ranking
