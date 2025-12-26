# Production Environment Configuration
# This file contains the actual values for the prd environment

project_id = "polar-scene-465223-f7"
region     = "europe-west1"

# Cloud Run Jobs Configuration
# Based on ingestion-config-prd.yaml
cloud_run_jobs = {
  # Spotify Jobs
  "spotify-recently-played" = {
    description = "Fetch recently played tracks from Spotify every 3 hours (prd)"
    service     = "spotify"
    cpu         = "1"
    memory      = "512Mi"
    timeout     = "600s"
    max_retries = 0
    schedule    = "0 8,11,14,17,20,23 * * *" # Every 3 hours from 9am to midnight Paris time
    env_vars = {
      SERVICE     = "spotify"
      SCOPE       = "recently_played"
      DESTINATION = "gs://ela-dp-prd/spotify/landing/"
      LIMIT       = "50"
      LOG_LEVEL   = "INFO"
    }
    secrets = {
      SPOTIFY_CLIENT_ID = {
        secret = "SPOTIFY_CLIENT_ID"
      }
      SPOTIFY_CLIENT_SECRET = {
        secret = "SPOTIFY_CLIENT_SECRET"
      }
      SPOTIFY_REFRESH_TOKEN = {
        secret = "SPOTIFY_REFRESH_TOKEN"
      }
      SPOTIFY_REDIRECT_URI = {
        secret = "SPOTIFY_REDIRECT_URI"
      }
    }
    labels = {
      service = "spotify"
      type    = "fetcher"
    }
  }

  "spotify-saved-tracks" = {
    description = "Fetch saved tracks from Spotify daily (prd)"
    service     = "spotify"
    cpu         = "1"
    memory      = "512Mi"
    timeout     = "600s"
    max_retries = 0
    schedule    = "0 11,23 * * *" # Daily at midday and midnight Paris time
    env_vars = {
      SERVICE     = "spotify"
      SCOPE       = "saved_tracks"
      DESTINATION = "gs://ela-dp-prd/spotify/landing/"
      LIMIT       = "50"
      LOG_LEVEL   = "INFO"
    }
    secrets = {
      SPOTIFY_CLIENT_ID = {
        secret = "SPOTIFY_CLIENT_ID"
      }
      SPOTIFY_CLIENT_SECRET = {
        secret = "SPOTIFY_CLIENT_SECRET"
      }
      SPOTIFY_REFRESH_TOKEN = {
        secret = "SPOTIFY_REFRESH_TOKEN"
      }
      SPOTIFY_REDIRECT_URI = {
        secret = "SPOTIFY_REDIRECT_URI"
      }
    }
    labels = {
      service = "spotify"
      type    = "fetcher"
    }
  }

  "spotify-saved-albums" = {
    description = "Fetch saved albums from Spotify daily (prd)"
    service     = "spotify"
    cpu         = "1"
    memory      = "512Mi"
    timeout     = "600s"
    max_retries = 0
    schedule    = "0 11,23 * * *" # Daily at midday and midnight Paris time
    env_vars = {
      SERVICE     = "spotify"
      SCOPE       = "saved_albums"
      DESTINATION = "gs://ela-dp-prd/spotify/landing/"
      LIMIT       = "50"
      LOG_LEVEL   = "INFO"
    }
    secrets = {
      SPOTIFY_CLIENT_ID = {
        secret = "SPOTIFY_CLIENT_ID"
      }
      SPOTIFY_CLIENT_SECRET = {
        secret = "SPOTIFY_CLIENT_SECRET"
      }
      SPOTIFY_REFRESH_TOKEN = {
        secret = "SPOTIFY_REFRESH_TOKEN"
      }
      SPOTIFY_REDIRECT_URI = {
        secret = "SPOTIFY_REDIRECT_URI"
      }
    }
    labels = {
      service = "spotify"
      type    = "fetcher"
    }
  }

  # Garmin Job
  "garmin-fetch" = {
    description = "Fetch all Garmin data daily (prd)"
    service     = "garmin"
    cpu         = "1"
    memory      = "512Mi"
    timeout     = "600s"
    max_retries = 0
    schedule    = "0 11 * * *" # Daily at 11:00 UTC
    env_vars = {
      SERVICE     = "garmin"
      SCOPE       = "all"
      DESTINATION = "gs://ela-dp-prd/garmin/landing/"
      DAYS        = "2"
      LOG_LEVEL   = "INFO"
    }
    secrets = {
      GARMIN_USERNAME = {
        secret = "GARMIN_USERNAME"
      }
      GARMIN_PASSWORD = {
        secret = "GARMIN_PASSWORD"
      }
    }
    labels = {
      service = "garmin"
      type    = "fetcher"
    }
  }

  # Chess Jobs
  "chess-player-stats" = {
    description = "Fetch player statistics from Chess.com (prd)"
    service     = "chess"
    cpu         = "1"
    memory      = "512Mi"
    timeout     = "600s"
    max_retries = 0
    schedule    = "0 21 * * *" # Daily at 21:00 UTC (22:00 Paris time)
    env_vars = {
      SERVICE     = "chess"
      SCOPE       = "player_stats"
      DESTINATION = "gs://ela-dp-prd/chess/landing/"
      LOG_LEVEL   = "INFO"
    }
    labels = {
      service = "chess"
      type    = "fetcher"
    }
  }

  "chess-games" = {
    description = "Fetch recent games from Chess.com (prd)"
    service     = "chess"
    cpu         = "1"
    memory      = "512Mi"
    timeout     = "600s"
    max_retries = 0
    schedule    = "0 21 * * *" # Daily at 21:00 UTC (22:00 Paris time)
    env_vars = {
      SERVICE     = "chess"
      SCOPE       = "games"
      DESTINATION = "gs://ela-dp-prd/chess/landing/"
      DAYS        = "1"
      LOG_LEVEL   = "INFO"
    }
    labels = {
      service = "chess"
      type    = "fetcher"
    }
  }

  # Ingestion Jobs
  "spotify-ingest" = {
    description = "Ingest Spotify data from GCS to BigQuery (prd)"
    service     = "spotify"
    cpu         = "1"
    memory      = "1Gi"
    timeout     = "900s"
    max_retries = 0
    schedule    = "30 * * * *" # 30 minutes after hourly fetch
    env_vars = {
      MODE = "ingest"
      ENV  = "prd"
    }
    labels = {
      service = "spotify"
      type    = "ingestor"
    }
  }

  "garmin-ingest" = {
    description = "Ingest Garmin data from GCS to BigQuery (prd)"
    service     = "garmin"
    cpu         = "1"
    memory      = "1Gi"
    timeout     = "900s"
    max_retries = 0
    schedule    = "30 */2 * * *" # 30 minutes after data fetch
    env_vars = {
      MODE = "ingest"
      ENV  = "prd"
    }
    labels = {
      service = "garmin"
      type    = "ingestor"
    }
  }

  "chess-ingest" = {
    description = "Ingest Chess.com data from GCS to BigQuery (prd)"
    service     = "chess"
    cpu         = "1"
    memory      = "1Gi"
    timeout     = "900s"
    max_retries = 0
    schedule    = "0 22 * * *" # Daily at 22:00 UTC (23:00 Paris time) - 1h after fetch
    env_vars = {
      MODE = "ingest"
      ENV  = "prd"
    }
    labels = {
      service = "chess"
      type    = "ingestor"
    }
  }

  # DBT Jobs
  "dbt-run-spotify" = {
    description = "Run dbt transformations for Spotify lake models (prd)"
    service     = "dbt"
    cpu         = "2"
    memory      = "2Gi"
    timeout     = "1200s"
    max_retries = 0
    schedule    = "0 7,12,16,20,23 * * *" # 5 times per day
    env_vars = {
      MODE        = "dbt"
      DBT_COMMAND = "run"
      DBT_SELECT  = "spotify"
      DBT_TARGET  = "prd"
    }
    labels = {
      service = "dbt"
      type    = "transformation"
      source  = "spotify"
    }
  }

  "dbt-run-garmin" = {
    description = "Run dbt transformations for Garmin lake models (prd)"
    service     = "dbt"
    cpu         = "2"
    memory      = "2Gi"
    timeout     = "1200s"
    max_retries = 0
    schedule    = "0 7,12,16,20,23 * * *" # 5 times per day
    env_vars = {
      MODE        = "dbt"
      DBT_COMMAND = "run"
      DBT_SELECT  = "garmin"
      DBT_TARGET  = "prd"
    }
    labels = {
      service = "dbt"
      type    = "transformation"
      source  = "garmin"
    }
  }

  "dbt-run-chess" = {
    description = "Run dbt transformations for Chess.com lake models (prd)"
    service     = "dbt"
    cpu         = "2"
    memory      = "2Gi"
    timeout     = "1200s"
    max_retries = 0
    schedule    = "0 7,12,16,20,23 * * *" # 5 times per day
    env_vars = {
      MODE        = "dbt"
      DBT_COMMAND = "run"
      DBT_SELECT  = "chess"
      DBT_TARGET  = "prd"
    }
    labels = {
      service = "dbt"
      type    = "transformation"
      source  = "chess"
    }
  }
}
