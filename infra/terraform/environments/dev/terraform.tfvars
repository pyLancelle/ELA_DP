# Development Environment Configuration
# This file contains the actual values for the dev environment

project_id = "polar-scene-465223-f7"
region     = "europe-west1"

# Cloud Run Jobs Configuration
# Add jobs as needed based on your ingestion-config-dev.yaml
cloud_run_jobs = {
  # Spotify Jobs
  "spotify-recently-played" = {
    description = "Fetch recently played tracks from Spotify (dev)"
    service     = "spotify"
    cpu         = "1"
    memory      = "512Mi"
    timeout     = "600s"
    max_retries = 0
    schedule    = "0 */3 * * *" # Every 3 hours
    env_vars = {
      SERVICE     = "spotify"
      SCOPE       = "recently_played"
      DESTINATION = "gs://ela-dp-dev/spotify/landing/"
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
    description = "Fetch saved tracks from Spotify (dev)"
    service     = "spotify"
    cpu         = "1"
    memory      = "512Mi"
    timeout     = "600s"
    max_retries = 0
    schedule    = "0 12 * * *" # Daily at noon
    env_vars = {
      SERVICE     = "spotify"
      SCOPE       = "saved_tracks"
      DESTINATION = "gs://ela-dp-dev/spotify/landing/"
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
    description = "Fetch all Garmin data (dev)"
    service     = "garmin"
    cpu         = "1"
    memory      = "512Mi"
    timeout     = "600s"
    max_retries = 0
    schedule    = "0 10 * * *" # Daily at 10am
    env_vars = {
      SERVICE     = "garmin"
      SCOPE       = "all"
      DESTINATION = "gs://ela-dp-dev/garmin/landing/"
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

  # DBT Jobs
  "dbt-run-spotify" = {
    description = "Run dbt transformations for Spotify data (dev)"
    service     = "dbt"
    cpu         = "2"
    memory      = "2Gi"
    timeout     = "1200s"
    max_retries = 0
    schedule    = "30 */3 * * *" # 30 minutes after each fetch
    env_vars = {
      MODE        = "dbt"
      DBT_COMMAND = "run"
      DBT_SELECT  = "spotify"
      DBT_TARGET  = "dev"
    }
    labels = {
      service = "dbt"
      type    = "transformation"
      source  = "spotify"
    }
  }

  "dbt-run-garmin" = {
    description = "Run dbt transformations for Garmin data (dev)"
    service     = "dbt"
    cpu         = "2"
    memory      = "2Gi"
    timeout     = "1200s"
    max_retries = 0
    schedule    = "30 10 * * *" # 30 minutes after Garmin fetch
    env_vars = {
      MODE        = "dbt"
      DBT_COMMAND = "run"
      DBT_SELECT  = "garmin"
      DBT_TARGET  = "dev"
    }
    labels = {
      service = "dbt"
      type    = "transformation"
      source  = "garmin"
    }
  }
}
