{{
  config(
      tags=['spotify', 'lake']
  )
}}

SELECT
    PLAYEDAT,
    CONTEXTTYPE,
    CONTEXTURI,
    CONTEXTHREF,
    CONTEXTEXTERNALURL,
    TRACKID,
    TRACKNAME,
    TRACKURI,
    TRACKEXTERNALURL,
    TRACKDURATIONMS,
    TRACKPOPULARITY,
    TRACKEXPLICIT,
    TRACKHREF,
    TRACKTYPE,
    TRACKPREVIEWURL,
    TRACKDISCNUMBER,
    TRACKNUMBER,
    TRACKISLOCAL,
    TRACKISRC,
    ALBUMID,
    ALBUMNAME,
    ALBUM_TYPE,
    ALBUMRELEASEDATE,
    ALBUMRELEASEDATEPRECISION,
    ALBUMTOTALTRACKS,
    ALBUMURI,
    ALBUMEXTERNALURL,
    ALBUMHREF,
    ALBUMTYPE,
    -- Extract album images from raw_data JSON
    COALESCE(
        ALBUMIMAGEURL,
        JSON_EXTRACT_SCALAR(raw_data, '$.track.album.images[0].url')
    ) AS ALBUMIMAGEURL,
    COALESCE(
        ALBUMIMAGEHEIGHT,
        CAST(JSON_EXTRACT_SCALAR(raw_data, '$.track.album.images[0].height') AS INT64)
    ) AS ALBUMIMAGEHEIGHT,
    COALESCE(
        ALBUMIMAGEWIDTH,
        CAST(JSON_EXTRACT_SCALAR(raw_data, '$.track.album.images[0].width') AS INT64)
    ) AS ALBUMIMAGEWIDTH,
    -- Additional image sizes
    JSON_EXTRACT_SCALAR(raw_data, '$.track.album.images[1].url') AS ALBUMIMAGEURLMEDIUM,
    JSON_EXTRACT_SCALAR(raw_data, '$.track.album.images[2].url') AS ALBUMIMAGEURLSMALL,
    ALBUMARTISTS,
    ARTISTS,
    DP_INSERTED_AT
FROM {{ source('spotify','recently_played') }}
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY playedat ORDER BY dp_inserted_at DESC) = 1
