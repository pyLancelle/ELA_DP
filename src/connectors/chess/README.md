# Chess.com Connector

A comprehensive Chess.com data connector for the ELA DATAPLATFORM that fetches player data, game history, and chess performance analytics.

## Features

- **No Authentication Required**: Uses Chess.com's public API
- **Multiple Data Types**: Profile, stats, games, clubs, tournaments
- **Rate Limited**: Respects API limits with configurable delays
- **JSONL Output**: Generates structured data files for ingestion
- **Following ELA Patterns**: Consistent with Spotify/Garmin connectors

## Quick Start

### 1. Fetch Player Data

```bash
# Get profile and stats for a player
python -m src.connectors.chess.chess_fetch hikaru --data-types player_profile player_stats

# Get recent games (last 30 days)
python -m src.connectors.chess.chess_fetch hikaru --data-types games --days 30

# Get all data types
python -m src.connectors.chess.chess_fetch myusername
```

### 2. Ingest to BigQuery

```bash
# Upload JSONL files to GCS bucket (gs://ela-dp-dev/chess/landing/)
gsutil cp *.jsonl gs://ela-dp-dev/chess/landing/

# Ingest to BigQuery
python -m src.connectors.chess.chess_ingest --env dev
```

### 3. Run dbt Transformations

```bash
# Transform raw data into structured models
python -m src.connectors.chess.chess_dbt_run --env dev
```

## Data Pipeline Architecture

```
Chess.com API → Python Connector → JSONL files → GCS → BigQuery → dbt (lake → hub → product)
```

### Data Flow

1. **Fetch**: `chess_fetch.py` → JSONL files
2. **Ingest**: `chess_ingest.py` → BigQuery raw table
3. **Transform**: dbt models → Structured analytics tables

## Available Data Types

### Player Profile
- Basic information (name, title, location)
- Account status and verification
- Social metrics (followers, streaming)
- Activity timestamps

### Player Statistics
- Ratings by time control (daily, rapid, blitz, bullet)
- Win/loss/draw records
- Best ratings and dates
- FIDE rating
- Tactics and Puzzle Rush performance

### Games
- Complete game records with PGN
- Player ratings and results
- Opening information (ECO codes)
- Accuracy percentages
- Time controls and game duration

### Clubs
- Club membership information
- Club details and descriptions
- Member counts and activity

### Tournaments
- Tournament participation
- Tournament settings and rules
- Timing and status information

## Configuration Options

```bash
# Custom output directory
--output-dir /path/to/output

# Custom date range for games
--days 7  # Last 7 days

# Specific data types only
--data-types player_profile player_stats

# Custom rate limiting
--rate-limit 2.0  # 2 seconds between requests

# Timezone for timestamps
--timezone "America/New_York"
```

## dbt Models

### Lake Models (Raw JSON Storage)
- `lake_chess__svc_player_profile`
- `lake_chess__svc_player_stats` 
- `lake_chess__svc_games`
- `lake_chess__svc_clubs`
- `lake_chess__svc_tournaments`

### Hub Models (Structured Data)
- `hub_chess__player_profile`
- `hub_chess__player_stats`
- `hub_chess__games`
- `hub_chess__clubs`
- `hub_chess__tournaments`

### Product Models (Analytics)
- `product_chess__player_rating_evolution`
- `product_chess__game_analysis`

## Example Analytics Queries

### Rating Analysis
```sql
SELECT 
  username,
  primary_rating,
  skill_category,
  consistency_level
FROM `project.dp_product_dev.product_chess__player_rating_evolution`
WHERE primary_rating > 2000
```

### Game Performance
```sql
SELECT 
  username,
  time_class,
  AVG(performance_vs_expected) as avg_performance,
  COUNT(*) as games_played
FROM `project.dp_product_dev.product_chess__game_analysis`
WHERE start_time >= '2024-01-01'
GROUP BY username, time_class
```

## Error Handling

- **Rate Limiting**: Automatic delays between requests
- **Missing Data**: Graceful handling of missing fields
- **API Errors**: Retries and error logging
- **Schema Changes**: Raw JSON storage preserves all data

## Best Practices

1. **Rate Limiting**: Don't reduce rate-limit below 1.0 seconds
2. **Data Freshness**: Run daily for active players
3. **Historical Data**: Use longer date ranges for initial loads
4. **Monitoring**: Check logs for API errors or missing data

## Troubleshooting

### Common Issues

1. **No games found**: Check if username has public games
2. **Rate limit errors**: Increase `--rate-limit` value
3. **Missing tournaments/clubs**: Not all players participate in these
4. **Old timestamps**: Some Chess.com data uses Unix timestamps

### Debug Mode
```bash
python -m src.connectors.chess.chess_fetch username --log-level DEBUG
```

## Integration with ELA DATAPLATFORM

The Chess.com connector follows the same patterns as existing connectors:

- **Utils**: Uses shared `to_jsonl()` function
- **Architecture**: Raw JSON → Lake → Hub → Product
- **Environment**: Supports dev/prd environments
- **BigQuery**: Single raw table with JSON storage
- **dbt**: Layered transformations with tests

## API Reference

Chess.com Public API: https://www.chess.com/news/view/published-data-api
- No authentication required
- Rate limited (recommended 1+ second between requests)
- Comprehensive game and player data
- Historical data available