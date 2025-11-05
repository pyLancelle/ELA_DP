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
    TIME(fact.playedat) AS played_time
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
ORDER BY
    fact.playedat DESC
LIMIT 100
