WITH ranking AS (
    SELECT
        dim_tracks.trackid,
        dim_tracks.trackname,
        dim_tracks.all_artist_names,
        sum(dim_tracks.trackDURATIONMS) AS total_duration_ms,
        count(distinct fact_played.playedat) AS play_count,
        dim_tracks.trackExternalUrl,
        dim_albums.albumimageurl
    FROM {{ ref('hub_music__svc_fact_played')}} AS fact_played
    LEFT JOIN {{ ref('hub_music__svc_dim_tracks')}} AS dim_tracks
        ON fact_played.trackid = dim_tracks.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_albums')}} AS dim_albums
        ON dim_tracks.albumid = dim_albums.albumid
    WHERE DATE(fact_played.playedat) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY
        dim_tracks.trackid,
        dim_tracks.trackname,
        dim_tracks.all_artist_names,
        dim_tracks.trackExternalUrl,
        dim_albums.albumimageurl
)

SELECT
    ROW_NUMBER() OVER(ORDER BY total_duration_ms DESC) as rank,
    CASE
        WHEN LENGTH(trackname) > 40 THEN CONCAT(SUBSTR(trackname, 1, 37), '...')
        ELSE trackname
    END AS trackname,
    CASE
        WHEN LENGTH(all_artist_names) > 40 THEN CONCAT(SUBSTR(all_artist_names, 1, 37), '...')
        ELSE all_artist_names
    END AS all_artist_names,
    {{ ms_to_hms('total_duration_ms') }} AS total_duration,
    play_count,
    trackExternalUrl,
    albumimageurl,
    trackid
FROM ranking
ORDER BY rank
LIMIT 10