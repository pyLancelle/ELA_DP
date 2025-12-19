{{
  config(
      tags=['spotify', 'lake']
  )
}}

SELECT
    ADDEDAT,
    TRACKID,
    TRACKNAME,
    `type`,
    URI,
    HREF,
    DURATIONMS,
    EXPLICIT,
    POPULARITY,
    TRACKNUMBER,
    DISCNUMBER,
    ISLOCAL,
    ISPLAYABLE,
    PREVIEWURL,
    AVAILABLEMARKETS,
    EXTERNALURLS,
    EXTERNALIDS,
    ARTISTS,
    ALBUMID,
    ALBUMNAME,
    ALBUMTYPE,
    ALBUMRELEASEDATE,
    ALBUMRELEASEDATEPRECISION,
    ALBUMTOTALTRACKS,
    ALBUMTYPE2,
    ALBUMURI,
    ALBUMHREF,
    ALBUMISPLAYABLE,
    ALBUMAVAILABLEMARKETS,
    ALBUMEXTERNALURLS,
    ALBUMARTISTS,
    ALBUMIMAGES,
    -- Extract individual album image URLs for easier use
    ALBUMIMAGES[SAFE_OFFSET(0)].url AS albumImageUrlLarge,
    ALBUMIMAGES[SAFE_OFFSET(1)].url AS albumImageUrlMedium,
    ALBUMIMAGES[SAFE_OFFSET(2)].url AS albumImageUrlSmall,
    RAW_DATA,
    DP_INSERTED_AT AS _dp_inserted_at,
    SOURCE_FILE
FROM {{ source('spotify','saved_tracks') }}
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY addedat ORDER BY _dp_inserted_at DESC) = 1
