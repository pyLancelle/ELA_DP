# Spotify Artist Enrichment

## Overview

The Spotify Artist Enrichment system fetches detailed metadata for artists from the Spotify API and integrates it into the data platform. This enriches basic artist data (derived from listening history) with genres, popularity scores, follower counts, and profile images.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                      ENRICHMENT WORKFLOW                              │
└─────────────────────────────────────────────────────────────────────┘

1. Query BigQuery
   └─> Identify artists needing enrichment
       (missing from lake_spotify__normalized_artist_enrichment)

2. Fetch from Spotify API
   └─> spotify_artist_enrichment.py
       ├─> Batch requests (50 artists per call)
       └─> Output: {timestamp}_artist_enrichment.jsonl

3. Upload to GCS
   └─> gs://ela-dp-{env}/spotify/landing/

4. Ingest to BigQuery
   └─> spotify_ingest_v2.py (or spotify_ingest_auto.py)
       └─> Table: lake_spotify__normalized_artist_enrichment

5. Transform via dbt
   └─> lake_spotify__stg_artist_enrichment (deduplication)
       └─> lake_spotify__svc_artist_enrichment (incremental)
           └─> hub_music__stg_dim_artists (join with listening stats)
               └─> hub_music__svc_dim_artists (materialized)
```

## Data Enriched

For each artist, the following metadata is fetched from Spotify:

| Field | Type | Description |
|-------|------|-------------|
| `artistId` | STRING | Spotify artist ID (unique identifier) |
| `artistName` | STRING | Artist name |
| `genres` | ARRAY<STRING> | List of genres (e.g., ["indie rock", "alternative"]) |
| `popularity` | INT64 | Popularity score (0-100) |
| `followerCount` | INT64 | Total number of followers |
| `images` | ARRAY<RECORD> | Profile images (url, height, width) |
| `externalUrls` | RECORD | Spotify web player URL |
| `enrichedAt` | TIMESTAMP | When data was fetched |

## Execution Modes

### 1. Backfill Mode (Initial Run)

Enriches **all** artists currently in `hub_music__svc_dim_artists` that don't have enrichment data yet.

```bash
# Step 1: Fetch artist data and upload to GCS automatically
# Note: --dataset should be the LAKE dataset (e.g., dp_lake_dev)
# The script will automatically derive the hub dataset (e.g., dp_hub_dev)
python src/connectors/spotify/spotify_artist_enrichment.py \
  --mode backfill \
  --output-dir ./output \
  --project-id polar-scene-465223-f7 \
  --dataset dp_lake_dev \
  --gcs-bucket ela-dp-dev

# Alternative: Without automatic upload (manual gsutil copy)
python src/connectors/spotify/spotify_artist_enrichment.py \
  --mode backfill \
  --output-dir ./output \
  --project-id polar-scene-465223-f7 \
  --dataset dp_lake_dev

# Then manually upload:
gsutil cp ./output/*_artist_enrichment.jsonl gs://ela-dp-dev/spotify/landing/

# Step 3: Ingest to BigQuery
python -m src.connectors.spotify.spotify_ingest_v2 \
  --config artist_enrichment \
  --env dev

# Step 4: Run dbt models
cd src/dbt_dataplatform
dbt run --select lake_spotify__stg_artist_enrichment+
```

**When to use**:
- Initial setup of enrichment system
- After major data migrations
- When you want to refresh all artist metadata

### 2. Incremental Mode (Weekly Runs)

Enriches **only** artists discovered in the last 7 days.

```bash
# Step 1: Fetch new artist data and upload to GCS automatically
python src/connectors/spotify/spotify_artist_enrichment.py \
  --mode incremental \
  --output-dir ./output \
  --project-id polar-scene-465223-f7 \
  --dataset dp_lake_dev \
  --gcs-bucket ela-dp-dev

# Step 3: Auto-ingest (detects and processes all data types)
python -m src.connectors.spotify.spotify_ingest_auto --env dev

# Step 4: Run dbt models (incremental)
cd src/dbt_dataplatform
dbt run --select lake_spotify__stg_artist_enrichment+
```

**When to use**:
- Weekly scheduled runs (recommended: Sunday mornings)
- After ingesting new listening history
- Continuous enrichment of newly discovered artists

## Scheduling

### Recommended Schedule

**Sunday 7:00 AM (Paris Time)** - Weekly incremental enrichment

```bash
#!/bin/bash
# Weekly artist enrichment job

set -e

export GCP_PROJECT_ID="polar-scene-465223-f7"
export GCP_SERVICE_ACCOUNT_KEY="/path/to/service-account-key.json"

echo "=== Starting weekly artist enrichment ==="

# Fetch incremental data and upload to GCS
python src/connectors/spotify/spotify_artist_enrichment.py \
  --mode incremental \
  --output-dir /tmp/spotify_enrichment \
  --project-id "$GCP_PROJECT_ID" \
  --dataset dp_lake_prd \
  --gcs-bucket ela-dp-prd

# Auto-ingest all pending Spotify data
python -m src.connectors.spotify.spotify_ingest_auto --env prd

# Run dbt
cd src/dbt_dataplatform
dbt run --select lake_spotify__stg_artist_enrichment+ --target prd

echo "=== Enrichment completed ==="

# Cleanup
rm -f /tmp/spotify_enrichment/*.jsonl
```

### Cloud Scheduler (GCP)

If using Cloud Scheduler, create a job with:

```yaml
schedule: "0 7 * * 0"  # Every Sunday at 7:00 AM
timezone: "Europe/Paris"
target: Cloud Run / Cloud Functions / Compute Engine
command: [see script above]
```

## Dry Run / Testing

### Test artist query without fetching

```bash
python src/connectors/spotify/spotify_artist_enrichment.py \
  --mode backfill \
  --dry-run \
  --project-id polar-scene-465223-f7 \
  --dataset dp_lake_dev
```

This will:
- Query BigQuery for artists needing enrichment
- Display the first 10 artists
- **Not** call the Spotify API
- Useful for verifying query logic and estimating batch size

### Test ingestion without inserting

```bash
python -m src.connectors.spotify.spotify_ingest_v2 \
  --config artist_enrichment \
  --env dev \
  --dry-run
```

This will:
- Scan GCS landing folder
- Parse and validate JSONL files
- **Not** insert into BigQuery or move files

## Monitoring & Validation

### Check enrichment coverage

```sql
-- Artists without enrichment data
SELECT
  COUNT(*) AS unenriched_artist_count,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM `hub_music__svc_dim_artists`), 2) AS pct_missing
FROM `hub_music__svc_dim_artists` artists
LEFT JOIN `lake_spotify__normalized_artist_enrichment` enrich
  ON artists.artistId = enrich.artistId
WHERE enrich.artistId IS NULL;
```

### Check enrichment staleness

```sql
-- Artists not enriched in last 90 days
SELECT
  artistId,
  artistName,
  enrichedAt,
  DATE_DIFF(CURRENT_DATE(), DATE(enrichedAt), DAY) AS days_since_enrichment
FROM `lake_spotify__normalized_artist_enrichment`
WHERE enrichedAt < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
ORDER BY days_since_enrichment DESC
LIMIT 20;
```

### Verify latest enrichment run

```sql
-- Latest enrichment batch stats
SELECT
  DATE(enrichedAt) AS enrichment_date,
  COUNT(*) AS artist_count,
  COUNT(DISTINCT CASE WHEN ARRAY_LENGTH(genres) > 0 THEN artistId END) AS artists_with_genres,
  ROUND(AVG(popularity), 1) AS avg_popularity,
  ROUND(AVG(followerCount), 0) AS avg_followers
FROM `lake_spotify__normalized_artist_enrichment`
WHERE enrichedAt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY enrichment_date
ORDER BY enrichment_date DESC;
```

## Troubleshooting

### Issue: "No artists to enrich"

**Cause**: All artists already have enrichment data.

**Solution**:
- Check if enrichment data exists: `SELECT COUNT(*) FROM lake_spotify__normalized_artist_enrichment`
- Use `--mode backfill` to force re-enrichment
- Verify artists exist in `hub_music__svc_dim_artists`

### Issue: "Spotify API authentication failed"

**Cause**: Invalid or expired Spotify credentials.

**Solution**:
1. Verify `.env` file has correct credentials:
   - `SPOTIFY_CLIENT_ID`
   - `SPOTIFY_CLIENT_SECRET`
   - `SPOTIFY_REDIRECT_URI`
   - `SPOTIFY_REFRESH_TOKEN`
2. Generate new refresh token if needed
3. Ensure Spotify app is active (not in development mode restrictions)

### Issue: "BigQuery dataset not found"

**Cause**: Wrong dataset name or project ID.

**Solution**:
- Verify `--project-id` matches your GCP project
- Verify `--dataset` matches: `dp_lake_dev` (dev) or `dp_lake_prd` (prod)
- Check GCP permissions for service account

### Issue: "Could not enrich X artists"

**Cause**: Artists removed from Spotify or API rate limiting.

**Solution**:
- Check logs for specific artist IDs
- Verify artist IDs exist in Spotify (some may be deleted/unavailable)
- If rate limited, wait and retry (rate limits reset after 30 seconds)

## API Rate Limits

Spotify API limits:
- **Artists endpoint**: ~180 requests per minute
- **Batch size**: 50 artists per request
- **Effective rate**: ~9,000 artists per minute

For most use cases, rate limits are not an issue. The incremental mode typically enriches <100 artists per week.

## Cost Considerations

### Spotify API
- Free for authenticated requests
- No cost for artist metadata fetching

### BigQuery
- **Storage**: ~1 KB per artist (minimal)
- **Query cost**: Queries are small, negligible cost
- **Recommended**: Enable clustering on `artistId` for query optimization

### GCS
- **Storage**: Minimal (JSONL files archived after ingestion)
- **Network egress**: Negligible (small file sizes)

## Data Quality Notes

### Genres
- Some artists have **no genres** (instrumental, classical, very new artists)
- Genres are Spotify's curated tags (not user-generated)
- Genre taxonomy changes over time

### Popularity
- Updates frequently based on global play counts
- Score can fluctuate significantly week-to-week
- Not comparable across time periods (relative metric)

### Images
- Most artists have 3 sizes: 640x640, 320x320, 160x160
- Some artists have no images (unclaimed profiles, very new artists)
- Image URLs are CDN links (always available)

## Examples

### Query artists by genre

```sql
SELECT
  artistName,
  popularity,
  followerCount,
  ARRAY_TO_STRING(genres, ', ') AS genre_list
FROM `hub_music__svc_dim_artists`
WHERE 'indie rock' IN UNNEST(genres)
ORDER BY popularity DESC
LIMIT 20;
```

### Top genres in your library

```sql
SELECT
  genre,
  COUNT(DISTINCT artistId) AS artist_count,
  ROUND(AVG(popularity), 1) AS avg_popularity
FROM `hub_music__svc_dim_artists`,
UNNEST(genres) AS genre
GROUP BY genre
ORDER BY artist_count DESC
LIMIT 30;
```

### Artists with profile images

```sql
SELECT
  artistName,
  popularity,
  followerCount,
  imageurllarge AS profile_image
FROM `hub_music__svc_dim_artists`
WHERE imageurllarge IS NOT NULL
ORDER BY popularity DESC
LIMIT 20;
```

## Files Created

### Scripts
- `src/connectors/spotify/spotify_artist_enrichment.py` - Enrichment fetch script

### Configs
- `src/connectors/spotify/configs/artist_enrichment.yaml` - Ingestion configuration

### dbt Models
- `models/lake/staging/spotify/lake_spotify__stg_artist_enrichment.sql` - Staging (deduplication)
- `models/lake/service/spotify/lake_spotify__svc_artist_enrichment.sql` - Service (incremental)
- `models/hub/staging/music/hub_music__stg_dim_artists.sql` - Updated with enrichment join

### Updated Files
- `models/lake/staging/schema.yaml` - Added `artist_enrichment` source
- `src/connectors/spotify/spotify_ingest_auto.py` - Added pattern detection

## Support

For issues or questions:
1. Check logs in `/tmp/spotify_enrichment.log` (if configured)
2. Verify BigQuery tables exist and have data
3. Run dry-run mode to debug without API calls
4. Contact the data platform team

---

**Last Updated**: 2025-11-11
**Version**: 1.0.0
**Maintainer**: Data Platform Team
