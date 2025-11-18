WITH all_periods_raw AS (
    SELECT
        dim_artists.artistid,
        dim_artists.artistname,
        sum(dim_tracks.trackDURATIONMS) AS total_duration_ms,
        count(distinct fact_played.playedat) AS play_count,
        dim_artists.artistexternalurl,
        dim_artists.imageurllarge,
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
        dim_artists.artistexternalurl,
        dim_artists.imageurllarge

    UNION ALL

    SELECT
        dim_artists.artistid,
        dim_artists.artistname,
        sum(dim_tracks.trackDURATIONMS) AS total_duration_ms,
        count(distinct fact_played.playedat) AS play_count,
        dim_artists.artistexternalurl,
        dim_artists.imageurllarge,
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
        dim_artists.artistexternalurl,
        dim_artists.imageurllarge

    UNION ALL

    SELECT
        dim_artists.artistid,
        dim_artists.artistname,
        sum(dim_tracks.trackDURATIONMS) AS total_duration_ms,
        count(distinct fact_played.playedat) AS play_count,
        dim_artists.artistexternalurl,
        dim_artists.imageurllarge,
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
        dim_artists.artistexternalurl,
        dim_artists.imageurllarge

    UNION ALL

    SELECT
        dim_artists.artistid,
        dim_artists.artistname,
        sum(dim_tracks.trackDURATIONMS) AS total_duration_ms,
        count(distinct fact_played.playedat) AS play_count,
        dim_artists.artistexternalurl,
        dim_artists.imageurllarge,
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
        dim_artists.artistexternalurl,
        dim_artists.imageurllarge

    UNION ALL

    SELECT
        dim_artists.artistid,
        dim_artists.artistname,
        sum(dim_tracks.trackDURATIONMS) AS total_duration_ms,
        count(distinct fact_played.playedat) AS play_count,
        dim_artists.artistexternalurl,
        dim_artists.imageurllarge,
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
        dim_artists.artistexternalurl,
        dim_artists.imageurllarge
),

ranked_periods AS (
    SELECT
        artistid,
        artistname,
        total_duration_ms,
        play_count,
        artistexternalurl,
        imageurllarge,
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
    ranked_periods.imageurllarge as albumimageurl,
    ranked_periods.artistid
FROM ranked_periods
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
