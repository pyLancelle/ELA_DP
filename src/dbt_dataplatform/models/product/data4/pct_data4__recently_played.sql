WITH
artists_aggregated AS (
    SELECT
        bridge.trackid,
        string_agg(artists.artistname, ', ' ORDER BY bridge.artist_position) AS artist_names
    FROM
        {{ ref('hub_music__svc_bridge_tracks_artists') }} AS bridge
    INNER JOIN
        {{ ref('hub_music__svc_dim_artists') }} AS artists
        ON bridge.artistid = artists.artistid
    GROUP BY
        bridge.trackid
),

played_with_next AS (
    SELECT
        fact.playedat,
        fact.trackid,
        tracks.trackdurationms,
        LEAD(fact.playedat) OVER (ORDER BY fact.playedat) AS next_played_at
    FROM
        {{ ref('hub_music__svc_fact_played') }} AS fact
    INNER JOIN
        {{ ref('hub_music__svc_dim_tracks') }} AS tracks
        ON fact.trackid = tracks.trackid
)

SELECT
    fact.playedat,
    tracks.trackid,
    tracks.trackname,
    artists_aggregated.artist_names,
    albums.albumname,
    albums.albumimageurl,
    tracks.trackhref,
    DATE(fact.playedat) AS played_date,
    TIME(fact.playedat) AS played_time,
    tracks.trackdurationms,
    -- Calcul du temps réel d'écoute en millisecondes
    CASE
        WHEN pwn.next_played_at IS NOT NULL THEN
            LEAST(
                tracks.trackdurationms,
                TIMESTAMP_DIFF(pwn.next_played_at, fact.playedat, MILLISECOND)
            )
        ELSE tracks.trackdurationms
    END AS real_listen_duration_ms
FROM
    {{ ref('hub_music__svc_fact_played') }} AS fact
INNER JOIN
    {{ ref('hub_music__svc_dim_tracks') }} AS tracks
    ON fact.trackid = tracks.trackid
INNER JOIN
    {{ ref('hub_music__svc_dim_albums') }} AS albums
    ON tracks.albumid = albums.albumid
LEFT JOIN
    artists_aggregated
    ON tracks.trackid = artists_aggregated.trackid
LEFT JOIN
    played_with_next AS pwn
    ON fact.playedat = pwn.playedat 
    AND fact.trackid = pwn.trackid
WHERE CASE
        WHEN pwn.next_played_at IS NOT NULL THEN
            LEAST(
                tracks.trackdurationms,
                TIMESTAMP_DIFF(pwn.next_played_at, fact.playedat, MILLISECOND)
            )
        ELSE tracks.trackdurationms
    END > 30000
ORDER BY
    fact.playedat DESC
LIMIT 100
