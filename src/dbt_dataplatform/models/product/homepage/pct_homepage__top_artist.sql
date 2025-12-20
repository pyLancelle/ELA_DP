WITH ranking AS (
    SELECT
        dim_artists.artistid,
        dim_artists.artistname,
        sum(dim_tracks.trackDURATIONMS) AS total_duration_ms,
        count(distinct fact_played.playedat) AS play_count,
        dim_artists.artistexternalurl,
        dim_artists.imageurllarge
    FROM {{ ref('hub_music__svc_fact_played')}} AS fact_played
    LEFT JOIN {{ ref('hub_music__svc_bridge_tracks_artists')}} AS bridge_artists
        ON fact_played.trackid = bridge_artists.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_artists')}} AS dim_artists
        ON bridge_artists.artistid = dim_artists.artistid
    LEFT JOIN {{ ref('hub_music__svc_dim_tracks')}} AS dim_tracks
        ON fact_played.trackid = dim_tracks.trackid
    WHERE DATE(fact_played.playedat) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    AND bridge_artists.ARTIST_ROLE = 'primary'
    GROUP BY
        dim_artists.artistid,
        dim_artists.artistname,
        dim_artists.artistexternalurl,
        dim_artists.imageurllarge
)

SELECT
    ROW_NUMBER() OVER(ORDER BY total_duration_ms DESC) as rank,
    CASE
        WHEN LENGTH(artistname) > 40 THEN CONCAT(SUBSTR(artistname, 1, 37), '...')
        ELSE artistname
    END AS artistname,
    {{ ms_to_hms('total_duration_ms') }} AS total_duration,
    play_count,
    artistexternalurl,
    imageurllarge as albumimageurl,
    artistid
FROM ranking
ORDER BY rank
LIMIT 10
