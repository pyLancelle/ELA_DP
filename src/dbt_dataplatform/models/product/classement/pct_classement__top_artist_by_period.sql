WITH artist_top_album_image AS (
    SELECT
        bridge_artists.artistid,
        dim_albums.albumimageurl,
        ROW_NUMBER() OVER(PARTITION BY bridge_artists.artistid ORDER BY COUNT(distinct fact_played.playedat) DESC) as album_rank
    FROM {{ ref('hub_music__svc_fact_played')}} AS fact_played
    LEFT JOIN {{ ref('hub_music__svc_bridge_tracks_artists')}} AS bridge_artists
        ON fact_played.trackid = bridge_artists.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_tracks')}} AS dim_tracks
        ON fact_played.trackid = dim_tracks.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_albums')}} AS dim_albums
        ON dim_tracks.albumid = dim_albums.albumid
    WHERE bridge_artists.artist_role = 'primary'
    GROUP BY
        bridge_artists.artistid,
        dim_albums.albumimageurl
),

all_periods_raw AS (
    SELECT
        dim_artists.artistid,
        dim_artists.artistname,
        sum(dim_tracks.trackDURATIONMS) AS total_duration_ms,
        count(distinct fact_played.playedat) AS play_count,
        dim_artists.artistexternalurl,
        'yesterday' AS period
    FROM {{ ref('hub_music__svc_fact_played')}} AS fact_played
    LEFT JOIN {{ ref('hub_music__svc_bridge_tracks_artists')}} AS bridge_artists
        ON fact_played.trackid = bridge_artists.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_artists')}} AS dim_artists
        ON bridge_artists.artistid = dim_artists.artistid
    LEFT JOIN {{ ref('hub_music__svc_dim_tracks')}} AS dim_tracks
        ON fact_played.trackid = dim_tracks.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_albums')}} AS dim_albums
        ON dim_tracks.albumid = dim_albums.albumid
    WHERE DATE(fact_played.playedat) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    GROUP BY
        dim_artists.artistid,
        dim_artists.artistname,
        dim_artists.artistexternalurl

    UNION ALL

    SELECT
        dim_artists.artistid,
        dim_artists.artistname,
        sum(dim_tracks.trackDURATIONMS) AS total_duration_ms,
        count(distinct fact_played.playedat) AS play_count,
        dim_artists.artistexternalurl,
        'last_7_days' AS period
    FROM {{ ref('hub_music__svc_fact_played')}} AS fact_played
    LEFT JOIN {{ ref('hub_music__svc_bridge_tracks_artists')}} AS bridge_artists
        ON fact_played.trackid = bridge_artists.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_artists')}} AS dim_artists
        ON bridge_artists.artistid = dim_artists.artistid
    LEFT JOIN {{ ref('hub_music__svc_dim_tracks')}} AS dim_tracks
        ON fact_played.trackid = dim_tracks.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_albums')}} AS dim_albums
        ON dim_tracks.albumid = dim_albums.albumid
    WHERE DATE(fact_played.playedat) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY
        dim_artists.artistid,
        dim_artists.artistname,
        dim_artists.artistexternalurl

    UNION ALL

    SELECT
        dim_artists.artistid,
        dim_artists.artistname,
        sum(dim_tracks.trackDURATIONMS) AS total_duration_ms,
        count(distinct fact_played.playedat) AS play_count,
        dim_artists.artistexternalurl,
        'last_30_days' AS period
    FROM {{ ref('hub_music__svc_fact_played')}} AS fact_played
    LEFT JOIN {{ ref('hub_music__svc_bridge_tracks_artists')}} AS bridge_artists
        ON fact_played.trackid = bridge_artists.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_artists')}} AS dim_artists
        ON bridge_artists.artistid = dim_artists.artistid
    LEFT JOIN {{ ref('hub_music__svc_dim_tracks')}} AS dim_tracks
        ON fact_played.trackid = dim_tracks.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_albums')}} AS dim_albums
        ON dim_tracks.albumid = dim_albums.albumid
    WHERE DATE(fact_played.playedat) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY
        dim_artists.artistid,
        dim_artists.artistname,
        dim_artists.artistexternalurl

    UNION ALL

    SELECT
        dim_artists.artistid,
        dim_artists.artistname,
        sum(dim_tracks.trackDURATIONMS) AS total_duration_ms,
        count(distinct fact_played.playedat) AS play_count,
        dim_artists.artistexternalurl,
        'last_365_days' AS period
    FROM {{ ref('hub_music__svc_fact_played')}} AS fact_played
    LEFT JOIN {{ ref('hub_music__svc_bridge_tracks_artists')}} AS bridge_artists
        ON fact_played.trackid = bridge_artists.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_artists')}} AS dim_artists
        ON bridge_artists.artistid = dim_artists.artistid
    LEFT JOIN {{ ref('hub_music__svc_dim_tracks')}} AS dim_tracks
        ON fact_played.trackid = dim_tracks.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_albums')}} AS dim_albums
        ON dim_tracks.albumid = dim_albums.albumid
    WHERE DATE(fact_played.playedat) >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
    GROUP BY
        dim_artists.artistid,
        dim_artists.artistname,
        dim_artists.artistexternalurl

    UNION ALL

    SELECT
        dim_artists.artistid,
        dim_artists.artistname,
        sum(dim_tracks.trackDURATIONMS) AS total_duration_ms,
        count(distinct fact_played.playedat) AS play_count,
        dim_artists.artistexternalurl,
        'all_time' AS period
    FROM {{ ref('hub_music__svc_fact_played')}} AS fact_played
    LEFT JOIN {{ ref('hub_music__svc_bridge_tracks_artists')}} AS bridge_artists
        ON fact_played.trackid = bridge_artists.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_artists')}} AS dim_artists
        ON bridge_artists.artistid = dim_artists.artistid
    LEFT JOIN {{ ref('hub_music__svc_dim_tracks')}} AS dim_tracks
        ON fact_played.trackid = dim_tracks.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_albums')}} AS dim_albums
        ON dim_tracks.albumid = dim_albums.albumid
    GROUP BY
        dim_artists.artistid,
        dim_artists.artistname,
        dim_artists.artistexternalurl
),

ranked_periods AS (
    SELECT
        artistid,
        artistname,
        total_duration_ms,
        play_count,
        artistexternalurl,
        period,
        ROW_NUMBER() OVER(PARTITION BY period ORDER BY total_duration_ms DESC) as period_rank
    FROM all_periods_raw
)

SELECT
    ranked_periods.period_rank as rank,
    ranked_periods.period,
    CASE
        WHEN LENGTH(ranked_periods.artistname) > 40 THEN CONCAT(SUBSTR(ranked_periods.artistname, 1, 37), '...')
        ELSE ranked_periods.artistname
    END AS artistname,
    {{ ms_to_hms('ranked_periods.total_duration_ms') }} AS total_duration,
    ranked_periods.play_count,
    ranked_periods.artistexternalurl,
    artist_top_album_image.albumimageurl,
    ranked_periods.artistid
FROM ranked_periods
LEFT JOIN artist_top_album_image
    ON ranked_periods.artistid = artist_top_album_image.artistid
    AND artist_top_album_image.album_rank = 1
WHERE period_rank <= IF(period = 'yesterday', 10, 20)
ORDER BY
    CASE ranked_periods.period
        WHEN 'all_time' THEN 1
        WHEN 'last_365_days' THEN 2
        WHEN 'last_30_days' THEN 3
        WHEN 'last_7_days' THEN 4
        WHEN 'yesterday' THEN 5
    END,
    ranked_periods.period_rank
