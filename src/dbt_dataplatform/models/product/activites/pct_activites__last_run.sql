WITH
activities AS (
    SELECT * EXCEPT(polyline, timeseries)
    FROM {{ ref('hub_health__svc_activities') }}
    ORDER BY activityid DESC
    LIMIT 100
),

music_during_activities AS (
    SELECT
        a.activityid,
        ARRAY_AGG(
            STRUCT(
                m.playedAt AS played_at,
                t.trackName AS track_name,
                t.all_artist_names AS artists,
                alb.albumName AS album_name,
                alb.albumImageUrl AS album_image,
                t.trackDurationMS AS duration_ms,
                t.trackExternalUrl AS track_url
            )
            ORDER BY m.playedAt
        ) AS tracks_played
    FROM activities a
    LEFT JOIN {{ ref('hub_music__svc_fact_played') }} m
        ON m.playedAt >= TIMESTAMP(a.startTimeGMT)
        AND m.playedAt <= TIMESTAMP(a.endTimeGMT)
    LEFT JOIN {{ ref('hub_music__svc_dim_tracks') }} t
        ON m.trackid = t.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_albums') }} alb
        ON t.albumid = alb.albumid
    GROUP BY a.activityid
)

SELECT
    a.*,
    m.tracks_played
FROM activities a
LEFT JOIN music_during_activities m
    ON a.activityid = m.activityid
ORDER BY a.activityid DESC