WITH all_periods_raw AS (
    SELECT
        dim_tracks.trackid,
        dim_tracks.trackname,
        dim_tracks.all_artist_names,
        sum(dim_tracks.trackDURATIONMS) AS total_duration_ms,
        count(distinct fact_played.playedat) AS play_count,
        dim_tracks.trackExternalUrl,
        dim_albums.albumimageurl,
        'yesterday' AS period
    FROM {{ ref('hub_music__svc_fact_played')}} AS fact_played
    LEFT JOIN {{ ref('hub_music__svc_dim_tracks')}} AS dim_tracks
        ON fact_played.trackid = dim_tracks.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_albums')}} AS dim_albums
        ON dim_tracks.albumid = dim_albums.albumid
    WHERE DATE(fact_played.playedat) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    GROUP BY
        dim_tracks.trackid,
        dim_tracks.trackname,
        dim_tracks.all_artist_names,
        dim_tracks.trackExternalUrl,
        dim_albums.albumimageurl

    UNION ALL

    SELECT
        dim_tracks.trackid,
        dim_tracks.trackname,
        dim_tracks.all_artist_names,
        sum(dim_tracks.trackDURATIONMS) AS total_duration_ms,
        count(distinct fact_played.playedat) AS play_count,
        dim_tracks.trackExternalUrl,
        dim_albums.albumimageurl,
        'last_7_days' AS period
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

    UNION ALL

    SELECT
        dim_tracks.trackid,
        dim_tracks.trackname,
        dim_tracks.all_artist_names,
        sum(dim_tracks.trackDURATIONMS) AS total_duration_ms,
        count(distinct fact_played.playedat) AS play_count,
        dim_tracks.trackExternalUrl,
        dim_albums.albumimageurl,
        'last_30_days' AS period
    FROM {{ ref('hub_music__svc_fact_played')}} AS fact_played
    LEFT JOIN {{ ref('hub_music__svc_dim_tracks')}} AS dim_tracks
        ON fact_played.trackid = dim_tracks.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_albums')}} AS dim_albums
        ON dim_tracks.albumid = dim_albums.albumid
    WHERE DATE(fact_played.playedat) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY
        dim_tracks.trackid,
        dim_tracks.trackname,
        dim_tracks.all_artist_names,
        dim_tracks.trackExternalUrl,
        dim_albums.albumimageurl

    UNION ALL

    SELECT
        dim_tracks.trackid,
        dim_tracks.trackname,
        dim_tracks.all_artist_names,
        sum(dim_tracks.trackDURATIONMS) AS total_duration_ms,
        count(distinct fact_played.playedat) AS play_count,
        dim_tracks.trackExternalUrl,
        dim_albums.albumimageurl,
        'last_365_days' AS period
    FROM {{ ref('hub_music__svc_fact_played')}} AS fact_played
    LEFT JOIN {{ ref('hub_music__svc_dim_tracks')}} AS dim_tracks
        ON fact_played.trackid = dim_tracks.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_albums')}} AS dim_albums
        ON dim_tracks.albumid = dim_albums.albumid
    WHERE DATE(fact_played.playedat) >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
    GROUP BY
        dim_tracks.trackid,
        dim_tracks.trackname,
        dim_tracks.all_artist_names,
        dim_tracks.trackExternalUrl,
        dim_albums.albumimageurl

    UNION ALL

    SELECT
        dim_tracks.trackid,
        dim_tracks.trackname,
        dim_tracks.all_artist_names,
        sum(dim_tracks.trackDURATIONMS) AS total_duration_ms,
        count(distinct fact_played.playedat) AS play_count,
        dim_tracks.trackExternalUrl,
        dim_albums.albumimageurl,
        'all_time' AS period
    FROM {{ ref('hub_music__svc_fact_played')}} AS fact_played
    LEFT JOIN {{ ref('hub_music__svc_dim_tracks')}} AS dim_tracks
        ON fact_played.trackid = dim_tracks.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_albums')}} AS dim_albums
        ON dim_tracks.albumid = dim_albums.albumid
    GROUP BY
        dim_tracks.trackid,
        dim_tracks.trackname,
        dim_tracks.all_artist_names,
        dim_tracks.trackExternalUrl,
        dim_albums.albumimageurl
),

ranked_periods AS (
    SELECT
        trackid,
        trackname,
        all_artist_names,
        total_duration_ms,
        play_count,
        trackExternalUrl,
        albumimageurl,
        period,
        ROW_NUMBER() OVER(PARTITION BY period ORDER BY total_duration_ms DESC) as period_rank
    FROM all_periods_raw
)

SELECT
    period_rank as rank,
    period,
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
FROM ranked_periods
WHERE period_rank <= IF(period = 'yesterday', 10, 20)
ORDER BY
    CASE period
        WHEN 'all_time' THEN 1
        WHEN 'last_365_days' THEN 2
        WHEN 'last_30_days' THEN 3
        WHEN 'last_7_days' THEN 4
        WHEN 'yesterday' THEN 5
    END,
    period_rank
