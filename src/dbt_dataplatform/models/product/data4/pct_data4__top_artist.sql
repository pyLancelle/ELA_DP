WITH artist_stats AS (
    SELECT
        dim_artists.artistid,
        dim_artists.artistname,
        dim_tracks.all_artist_names,
        sum(dim_tracks.trackDURATIONMS) AS total_duration_ms,
        count(distinct fact_played.playedat) AS play_count,
        dim_artists.artistexternalurl
    FROM {{ ref('hub_music__svc_fact_played')}} AS fact_played
    LEFT JOIN {{ ref('hub_music__svc_bridge_tracks_artists')}} AS bridge_artists
        ON fact_played.trackid = bridge_artists.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_artists')}} AS dim_artists
        ON bridge_artists.artistid = dim_artists.artistid
    LEFT JOIN {{ ref('hub_music__svc_dim_tracks')}} AS dim_tracks
        ON fact_played.trackid = dim_tracks.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_albums')}} AS dim_albums
        ON dim_tracks.albumid = dim_albums.albumid
    -- WHERE bridge_artists.artist_role = 'primary'
    GROUP BY ALL
    ORDER BY total_duration_ms DESC
    LIMIT 20
),

ranking AS (
    SELECT
        *,
        ROW_NUMBER() OVER(ORDER BY total_duration_ms DESC) as rank
    FROM artist_stats
),

track_stats AS (
    SELECT
        dim_artists.artistid,
        dim_tracks.trackid,
        dim_tracks.trackname,
        sum(dim_tracks.trackDURATIONMS) AS track_duration_ms,
        count(distinct fact_played.playedat) AS track_play_count
    FROM {{ ref('hub_music__svc_fact_played')}} AS fact_played
    LEFT JOIN {{ ref('hub_music__svc_bridge_tracks_artists')}} AS bridge_artists
        ON fact_played.trackid = bridge_artists.trackid
    LEFT JOIN {{ ref('hub_music__svc_dim_artists')}} AS dim_artists
        ON bridge_artists.artistid = dim_artists.artistid
    LEFT JOIN {{ ref('hub_music__svc_dim_tracks')}} AS dim_tracks
        ON fact_played.trackid = dim_tracks.trackid
    WHERE dim_artists.artistid IN (SELECT artistid FROM ranking)
    GROUP BY ALL
),

top_tracks_per_artist AS (
    SELECT
        ts.artistid,
        ts.trackname,
        ts.track_duration_ms,
        ts.track_play_count,
        ROW_NUMBER() OVER (PARTITION BY ts.artistid ORDER BY ts.track_duration_ms DESC) as track_rank
    FROM track_stats ts
    QUALIFY track_rank <= 10
)

SELECT
    r.rank,
    CASE
        WHEN LENGTH(r.artistname) > 40 THEN CONCAT(SUBSTR(r.artistname, 1, 37), '...')
        ELSE r.artistname
    END AS artistname,
    {{ ms_to_hms('r.total_duration_ms') }} AS total_duration,
    r.play_count,
    r.artistexternalurl,
    ARRAY_AGG(
        STRUCT(
            tt.track_rank as rank,
            tt.trackname,
            ROUND((tt.track_duration_ms / r.total_duration_ms) * 100, 1) as percentage,
            {{ ms_to_hm('tt.track_duration_ms') }} as duration,
            tt.track_play_count as play_count
        )
        ORDER BY tt.track_rank
    ) as tracks
FROM ranking r
LEFT JOIN top_tracks_per_artist tt
    ON r.artistid = tt.artistid
GROUP BY r.rank, r.artistname, r.total_duration_ms, r.play_count, r.artistexternalurl
ORDER BY r.rank