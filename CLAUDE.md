# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ELA DATAPLATFORM is a personal data platform that ingests and transforms data from various services including Spotify, Strava, Todoist, and Garmin Connect. The project follows a modern data stack architecture with Python connectors for data ingestion and dbt for data transformation.

## Development Commands

### Python Environment
- **Setup**: `uv sync` (uses uv for dependency management)
- **Code formatting**: `black .` (configured via pre-commit)
- **Pre-commit hooks**: `pre-commit run --all-files`

### dbt Commands (from `src/dbt_dataplatform/`)
- **Run all models**: `dbt run`
- **Run tests**: `dbt test`
- **Build everything**: `dbt build`
- **Run specific model**: `dbt run --select model_name`
- **Run with full refresh**: `dbt run --full-refresh`

### Data Ingestion
- **Spotify data**: `python src/connectors/spotify/spotify_fetch.py [data_type]`
  - Recently played: `python src/connectors/spotify/spotify_fetch.py recently_played`
  - Saved tracks: `python src/connectors/spotify/spotify_fetch.py saved_tracks`
  - Playlists: `python src/connectors/spotify/spotify_fetch.py playlists`
  - User profile: `python src/connectors/spotify/spotify_fetch.py user_profile`
  - Top tracks: `python src/connectors/spotify/spotify_fetch.py top_tracks`
  - Top artists: `python src/connectors/spotify/spotify_fetch.py top_artists`
- **Strava data**: `python src/connectors/strava/strava.py`
- **Todoist data**: `python src/connectors/todoist/todoist.py`
- **Garmin data**: `python src/connectors/garmin/garmin_fetch.py`

## Architecture

### Data Flow
1. **Connectors** (`src/connectors/`) - Python scripts that fetch data from APIs and write to JSONL files
2. **Lake Layer** (`models/lake/`) - dbt models that stage raw data (materialized as tables)
3. **Hub Layer** (`models/hub/`) - dbt models that create normalized entities (materialized as views)
4. **Product Layer** (`models/product/`) - dbt models that create analytics views (materialized as views)

### Key Directories
- `src/connectors/` - Data ingestion scripts for each service
- `src/dbt_dataplatform/` - dbt project with data transformations
- `data/` - Raw JSONL files from connectors
- `logs/` - dbt execution logs

### Technology Stack
- **Data Warehouse**: Google BigQuery
- **Transformation**: dbt
- **Ingestion**: Python with service-specific libraries (spotipy, stravalib, garminconnect, requests)
- **Data Format**: JSONL for raw data storage
- **Package Management**: uv
- **Code Quality**: black, pre-commit

## Configuration

### Environment Variables Required
- `SPOTIFY_CLIENT_ID`, `SPOTIFY_CLIENT_SECRET`, `SPOTIFY_REDIRECT_URI`, `SPOTIFY_REFRESH_TOKEN`
- `STRAVA_CLIENT_ID`, `STRAVA_CLIENT_SECRET`, `STRAVA_REFRESH_TOKEN`
- `TODOIST_API_TOKEN`
- `GARMIN_USERNAME`, `GARMIN_PASSWORD`

### Key Files
- `pyproject.toml` - Python dependencies and project config
- `src/dbt_dataplatform/dbt_project.yml` - dbt project configuration
- `src/dbt_dataplatform/profiles.yml` - BigQuery connection settings
- `settings.yaml` - Connector configuration
- `.pre-commit-config.yaml` - Code quality hooks

## Development Notes

- All raw data is stored as timestamped JSONL files in `data/` directory
- Connectors use utility functions from `src/connectors/utils.py` for common operations
- dbt uses custom schema macro in `macros/get_schema.sql` for dataset organization
- BigQuery project: `polar-scene-465223-f7`
- Service account key: `gcs_key.json` (not in version control)

## GitHub Actions Workflows

- **Spotify Recently Played**: `spotify_dev_fetch.yaml` - Runs every 2 hours (50 tracks)
- **Spotify Saved Tracks**: `spotify_saved_tracks.yaml` - Runs weekly on Sundays (500 tracks)
- Both workflows upload to GCS bucket: `ela-dp-dev/spotify/landing`

## Bonus
- To help yourself, please use MCP:
    - Context7 for dbt / python / SQL
    - Spotify Web API for the API ingestion