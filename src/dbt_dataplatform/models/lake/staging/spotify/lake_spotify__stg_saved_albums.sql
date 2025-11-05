SELECT
    ADDEDAT,
    ALBUMID,
    ALBUMNAME,
    ALBUMTYPE,
    `type`,
    URI,
    HREF,
    TOTALTRACKS,
    RELEASEDATE,
    RELEASEDATEPRECISION,
    POPULARITY,
    LABEL,
    AVAILABLEMARKETS,
    GENRES,
    EXTERNALURLS,
    EXTERNALIDS,
    ALBUMARTISTS,
    IMAGES,
    -- Extract individual image URLs for easier use
    IMAGES[SAFE_OFFSET(0)].url AS imageUrlLarge,
    IMAGES[SAFE_OFFSET(1)].url AS imageUrlMedium,
    IMAGES[SAFE_OFFSET(2)].url AS imageUrlSmall,
    COPYRIGHTS,
    TRACKSSUMMARY,
    TRACKS,
    RAW_DATA,
    DP_INSERTED_AT,
    SOURCE_FILE
FROM {{ source('spotify','saved_albums') }}
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY addedat ORDER BY dp_inserted_at DESC) = 1
