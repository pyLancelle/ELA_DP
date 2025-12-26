# Generic Data Ingestor

Unified CLI for ingesting data from multiple services (Spotify, Garmin, Chess) into BigQuery.

## Architecture

The ingestor follows the same pattern as the fetcher:
- **Base class**: `IngestorAdapter` defines the interface for all ingestors
- **Adapters**: Service-specific adapters (`GarminIngestorAdapter`, `SpotifyIngestorAdapter`, `ChessIngestorAdapter`) wrap existing ingestion logic
- **Unified CLI**: Single entry point for all services

## Usage

### Command Line

```bash
# List available data types
python -m src.connectors.ingestor --list-types

# Ingest all Garmin data types
python -m src.connectors.ingestor --service garmin --env dev

# Ingest specific Spotify data types
python -m src.connectors.ingestor --service spotify --env dev --data-types recently_played,saved_tracks

# Ingest from multiple services
python -m src.connectors.ingestor --service garmin,spotify --env dev

# Dry run (validate without writing to BigQuery)
python -m src.connectors.ingestor --service garmin --env dev --dry-run
```

### Docker

The `docker-entrypoint.sh` has been updated to support both fetching and ingestion via the `MODE` environment variable:

```bash
# Fetch data (default mode)
docker run -e MODE=fetch -e SERVICE=garmin -e SCOPE=sleep,steps -e DESTINATION=gs://... IMAGE

# Ingest data
docker run -e MODE=ingest -e SERVICE=garmin -e ENV=dev IMAGE

# Multiple services
docker run -e MODE=ingest -e SERVICE=garmin,spotify -e ENV=dev IMAGE
```

### Environment Variables (Ingestion Mode)

- `MODE=ingest` - Required to enable ingestion mode
- `SERVICE` - Service(s) to ingest: `spotify`, `garmin`, `chess` (comma-separated)
- `ENV` - Environment: `dev` or `prd` (required)
- `DATA_TYPES` - Specific data types to ingest (optional, comma-separated)
- `DRY_RUN=true` - Validate without writing to BigQuery (optional)
- `LOG_LEVEL` - Logging level: `DEBUG`, `INFO`, `WARNING`, `ERROR` (optional)

## Architecture Details

### Service Adapters

Each service adapter:
1. **Wraps existing ingestion logic** - No code duplication, reuses existing scripts
2. **Provides unified interface** - Consistent API across all services
3. **Handles service-specific details** - Metric lists, table naming, schema management

### Data Flow

```
GCS Landing (gs://ela-dp-{env}/{service}/landing/)
    ↓
IngestorAdapter
    ↓
BigQuery (dp_normalized_{env} or dp_lake_{env})
    ↓
GCS Archive/Rejected (gs://ela-dp-{env}/{service}/archive|rejected/)
```

### Supported Services

| Service | Adapter | Data Types |
|---------|---------|------------|
| **Garmin** | `GarminIngestorAdapter` | 28 metrics (activities, sleep, heart_rate, etc.) |
| **Spotify** | `SpotifyIngestorAdapter` | 7 types (recently_played, saved_tracks, etc.) |
| **Chess** | `ChessIngestorAdapter` | 5 types (player_profile, games, etc.) |

## Integration with Existing Code

The adapters **wrap existing ingestion scripts** without modifying them:
- `GarminIngestorAdapter` → wraps `src/connectors/garmin/garmin_ingest.py`
- `SpotifyIngestorAdapter` → wraps `src/connectors/spotify/spotify_ingest.py`
- `ChessIngestorAdapter` → wraps `src/connectors/chess/chess_ingest.py`

This ensures:
- ✅ Backward compatibility with existing scripts
- ✅ No code duplication
- ✅ Unified interface for new workflows

## Examples

### GitHub Actions Integration

Update job commands in `ingestion-config-dev.yaml`:

```yaml
# Old approach
command: "python -m src.connectors.garmin.garmin_ingest --env dev"
command: "python -m src.connectors.spotify.spotify_ingest --env dev"

# New approach (recommended)
command: "python -m src.connectors.ingestor --service garmin --env dev"
command: "python -m src.connectors.ingestor --service spotify --env dev"
```

### Cloud Run / Kubernetes

```yaml
env:
  - name: MODE
    value: "ingest"
  - name: SERVICE
    value: "garmin"
  - name: ENV
    value: "prd"
  - name: GCP_PROJECT_ID
    valueFrom:
      secretKeyRef:
        name: gcp-credentials
        key: project-id
```

## Error Handling

The ingestor:
- Returns detailed `IngestResult` with success/failure counts
- Moves failed files to `rejected/` folder
- Provides clear error messages and logging
- Supports `--dry-run` for validation before actual ingestion

## Future Enhancements

- [ ] Add support for incremental ingestion (only new files)
- [ ] Add metrics/monitoring output (Prometheus format)
- [ ] Add notification hooks (email, Slack, etc.)
- [ ] Add automatic retry logic for transient failures
