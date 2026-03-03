{{ config(materialized='table', tags=['spotify', 'product']) }}

WITH streak_dates AS (
    SELECT DISTINCT
        bridge.artistid,
        DATE(fact.playedat) AS listen_date
    FROM {{ ref('hub_music__svc_fact_played') }} AS fact
    INNER JOIN {{ ref('hub_music__svc_bridge_tracks_artists') }} AS bridge
        ON fact.trackid = bridge.trackid
    WHERE bridge.artist_role = 'primary'
      AND bridge.artistid IS NOT NULL
),

numbered AS (
    SELECT
        artistid,
        DATE_DIFF(CURRENT_DATE(), listen_date, DAY) AS days_ago,
        ROW_NUMBER() OVER (PARTITION BY artistid ORDER BY listen_date DESC) AS rn
    FROM streak_dates
    WHERE listen_date <= CURRENT_DATE()
),

streaks AS (
    SELECT artistid, COUNT(*) AS current_streak
    FROM numbered
    WHERE days_ago = rn - 1   -- jours consécutifs jusqu'à aujourd'hui
    GROUP BY artistid
),

listening_stats AS (
    SELECT
        bridge.artistid,
        COUNT(DISTINCT fact.playedat)           AS total_play_count,
        SUM(tracks.trackdurationms)             AS total_duration_ms,
        COUNT(DISTINCT fact.trackid)            AS unique_tracks_count,
        COUNT(DISTINCT tracks.albumid)          AS unique_albums_count,
        MIN(DATE(fact.playedat))                AS first_played_date,
        MAX(DATE(fact.playedat))                AS last_played_date,
        COUNT(DISTINCT DATE(fact.playedat))     AS days_with_listens
    FROM {{ ref('hub_music__svc_fact_played') }} AS fact
    INNER JOIN {{ ref('hub_music__svc_bridge_tracks_artists') }} AS bridge
        ON fact.trackid = bridge.trackid
    INNER JOIN {{ ref('hub_music__svc_dim_tracks') }} AS tracks
        ON fact.trackid = tracks.trackid
    WHERE bridge.artist_role = 'primary'
      AND bridge.artistid IS NOT NULL
    GROUP BY bridge.artistid
)

SELECT
    a.artistid                                                                      AS artist_id,
    a.artistname                                                                    AS artist_name,
    a.artistexternalurl                                                             AS artist_url,
    a.imageurllarge                                                                 AS image_url,
    a.imageurlmedium                                                                AS image_url_medium,
    a.genres,
    a.popularity                                                                    AS spotify_popularity,
    a.followercount                                                                 AS follower_count,
    s.total_play_count                                                              AS total_plays,
    {{ ms_to_hms('s.total_duration_ms') }}                                         AS total_duration,
    s.total_duration_ms,
    s.unique_tracks_count                                                           AS unique_tracks,
    s.unique_albums_count                                                           AS unique_albums,
    CAST(s.first_played_date AS STRING)                                             AS first_heard,
    CAST(s.last_played_date AS STRING)                                              AS last_heard,
    s.days_with_listens,
    DATE_DIFF(CURRENT_DATE(), s.first_played_date, DAY)                            AS days_since_discovery,
    ROUND(
        100.0 * s.days_with_listens
        / NULLIF(DATE_DIFF(CURRENT_DATE(), s.first_played_date, DAY), 0),
        1
    )                                                                               AS consistency_score,
    ROUND(s.total_play_count / NULLIF(s.days_with_listens, 0), 1)                 AS avg_plays_per_active_day,
    COALESCE(str.current_streak, 0)                                                AS current_streak
FROM {{ ref('hub_music__svc_dim_artists') }} AS a
INNER JOIN listening_stats AS s ON a.artistid = s.artistid
LEFT JOIN streaks AS str ON a.artistid = str.artistid
