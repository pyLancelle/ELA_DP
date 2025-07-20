# Data Ingestion Orchestrator Documentation

## Overview

The Data Ingestion Orchestrator is a centralized GitHub Actions system that manages all data ingestion jobs for the ELA Data Platform. It provides a configuration-driven approach to scheduling and executing data extraction from multiple sources including Spotify, Strava, Garmin, and Todoist.

## 🏗️ Architecture

### Components

1. **Configuration File (`ingestion-config.yaml`)**: Defines all jobs, schedules, and service configurations
2. **Orchestrator Workflow (`.github/workflows/data-ingestion-orchestrator.yaml`)**: Main GitHub Actions workflow
3. **Python Dependencies**: CRON parsing and scheduling logic
4. **Service Connectors**: Individual Python scripts for each data source

### Data Flow

```
⏰ Schedule Trigger / Manual Trigger
    ↓
🔍 Job Determination (Python CRON logic)
    ↓
🚀 Parallel Job Execution (Matrix Strategy)
    ↓
📁 Data Upload to GCS
    ↓
📊 Summary Report Generation
```

## 📋 Configuration Structure

### Global Settings
```yaml
global:
  timezone: "Europe/Paris"
  retry_attempts: 2
  max_parallel_jobs: 3
  gcs_bucket_dev: "ela-dp-dev"
  gcs_bucket_prd: "ela-dp-prd"
  timeout_minutes: 30
```

### Job Configuration
Each job includes:
- **service**: Data source (spotify, strava, garmin, todoist)
- **data_type**: Specific data type to extract
- **description**: Human-readable description
- **cron**: CRON expression for scheduling
- **command**: Python command to execute
- **environment**: Target environment (dev/prd)
- **enabled**: Enable/disable flag
- **dependencies**: Required environment variables

### Job Groups
Pre-defined groups for batch execution:
- `spotify_all`: All Spotify data sources
- `fitness_all`: All fitness data sources (Strava + Garmin)
- `productivity_all`: All productivity data sources (Todoist)
- `daily_jobs`: Jobs that run daily
- `weekly_jobs`: Jobs that run weekly
- `monthly_jobs`: Jobs that run monthly

## 🕐 Scheduling Logic

### Automatic Execution
- Workflow runs **every hour** (CRON: `0 * * * *`)
- Checks if any job's CRON schedule indicates it should run
- Uses **1-hour tolerance window** for job execution
- Executes jobs in parallel (max 3 concurrent)

### CRON Schedule Examples
```yaml
"0 */2 * * *"    # Every 2 hours
"0 12 * * 0"     # Weekly on Sundays at 12:00 UTC
"0 8 1 * *"      # Monthly on 1st day at 8:00 UTC
"0 */3 * * *"    # Every 3 hours
"0 7 * * *"      # Daily at 7:00 UTC
```

### Manual Execution Options
1. **Specific Job**: Run individual job by ID
2. **Job Group**: Run all jobs in a group
3. **Force Run**: Execute all jobs regardless of schedule
4. **Environment**: Choose dev or prd environment

## 🚀 Current Job Schedule

### Spotify Jobs
| Job | Data Type | Schedule | Description |
|-----|-----------|----------|-------------|
| `spotify_recently_played` | recently_played | Every 2 hours | Recent listening history |
| `spotify_saved_tracks` | saved_tracks | Weekly (Sunday 12:00) | Liked songs library |
| `spotify_saved_albums` | saved_albums | Weekly (Sunday 12:00) | Saved albums library |
| `spotify_playlists` | playlists | Monthly (1st, 8:00) | User playlists |
| `spotify_top_tracks` | top_tracks | Monthly (1st, 9:00) | Top tracks analysis |
| `spotify_top_artists` | top_artists | Monthly (1st, 10:00) | Top artists analysis |

### Strava Jobs
| Job | Data Type | Schedule | Description |
|-----|-----------|----------|-------------|
| `strava_activities` | activities | Every 6 hours | Workout activities |
| `strava_athlete` | athlete | Weekly (Monday 6:00) | Athlete profile |

### Garmin Jobs
| Job | Data Type | Schedule | Description |
|-----|-----------|----------|-------------|
| `garmin_activities` | activities | Every 4 hours | Workout activities |
| `garmin_sleep` | sleep | Daily (8:00) | Sleep data |
| `garmin_heart_rate` | heart_rate | Every 8 hours | Heart rate data |

### Todoist Jobs
| Job | Data Type | Schedule | Description |
|-----|-----------|----------|-------------|
| `todoist_tasks` | tasks | Every 3 hours | Task management data |
| `todoist_projects` | projects | Daily (7:00) | Project structure |

## 🔧 Setup Instructions

### 1. Repository Setup
Ensure these files are in your repository:
- `ingestion-config.yaml` (root directory)
- `.github/workflows/data-ingestion-orchestrator.yaml`
- Updated `pyproject.toml` with required dependencies

### 2. GitHub Secrets Configuration
Configure these secrets in your GitHub repository:

#### Spotify
- `SPOTIFY_CLIENT_ID`
- `SPOTIFY_CLIENT_SECRET`
- `SPOTIFY_REDIRECT_URI`
- `SPOTIFY_REFRESH_TOKEN`

#### Strava
- `STRAVA_CLIENT_ID`
- `STRAVA_CLIENT_SECRET`
- `STRAVA_REFRESH_TOKEN`

#### Garmin
- `GARMIN_USERNAME`
- `GARMIN_PASSWORD`

#### Todoist
- `TODOIST_API_TOKEN`

#### Google Cloud
- `GCS_SERVICE_ACCOUNT_KEY` (base64 encoded service account JSON)
- `GCP_PROJECT_ID`

### 3. Testing
Run the validation script to test your configuration:
```bash
python test_ingestion_config.py
```

## 📊 Usage Examples

### Manual Job Execution

#### Run Specific Job
1. Go to Actions → Data Ingestion Orchestrator
2. Click "Run workflow"
3. Enter job ID: `spotify_recently_played`
4. Click "Run workflow"

#### Run Job Group
1. Go to Actions → Data Ingestion Orchestrator
2. Click "Run workflow"
3. Select job group: `spotify_all`
4. Click "Run workflow"

#### Force Run All Jobs
1. Go to Actions → Data Ingestion Orchestrator
2. Click "Run workflow"
3. Check "Force run all jobs"
4. Click "Run workflow"

### Monitoring

#### Execution Summary
Each workflow run generates a summary showing:
- Execution timestamp and trigger type
- Environment and job count
- Individual job status
- Links to detailed logs

#### Error Handling
- Individual job failures don't stop other jobs
- Failed jobs upload error logs as artifacts
- Comprehensive error reporting in summary

## 🔄 Adding New Data Sources

### 1. Add Service Configuration
```yaml
services:
  new_service:
    base_path: "src/connectors/new_service"
    fetch_script: "new_service_fetch.py"
    supported_data_types: ["data_type1", "data_type2"]
```

### 2. Add Job Configuration
```yaml
jobs:
  new_service_job:
    service: "new_service"
    data_type: "data_type1"
    description: "Fetch data from new service"
    cron: "0 */6 * * *"  # Every 6 hours
    command: "python -m src.connectors.new_service.new_service_fetch data_type1"
    environment: "dev"
    enabled: true
    dependencies: ["NEW_SERVICE_API_KEY"]
```

### 3. Update Job Groups (Optional)
```yaml
job_groups:
  new_group:
    description: "New service data sources"
    jobs: ["new_service_job"]
```

### 4. Add Required Secrets
Add necessary API keys/tokens to GitHub repository secrets.

## 🐛 Troubleshooting

### Common Issues

#### Jobs Not Running
1. Check CRON expression validity
2. Verify job is enabled (`enabled: true`)
3. Check environment matching (`dev` vs `prd`)
4. Validate timezone configuration

#### Authentication Failures
1. Verify all required secrets are configured
2. Check secret names match dependency list
3. Validate API tokens haven't expired
4. Test individual connectors locally

#### Upload Failures
1. Check GCS service account permissions
2. Verify bucket names and paths
3. Validate GCP project configuration
4. Check file generation by connectors

### Debugging Steps
1. **Review workflow logs**: Check individual job execution logs
2. **Download error artifacts**: Failed jobs upload detailed error reports
3. **Test locally**: Run individual connector commands locally
4. **Validate configuration**: Use `test_ingestion_config.py` script
5. **Check dependencies**: Ensure all required packages are installed

## 📈 Monitoring and Optimization

### Performance Metrics
- Monitor job execution times
- Track success/failure rates
- Analyze data volume trends
- Review resource usage

### Optimization Opportunities
- Adjust parallel job limits based on performance
- Fine-tune CRON schedules based on data freshness needs
- Optimize individual connector performance
- Consider cost vs. freshness trade-offs

## 🔒 Security Considerations

### Best Practices
- Store all sensitive data in GitHub Secrets
- Use least-privilege service accounts
- Regularly rotate API tokens and keys
- Monitor for unauthorized access attempts
- Review and audit secret access logs

### Data Protection
- Ensure GDPR compliance for personal data
- Implement data retention policies
- Use encrypted storage and transmission
- Regular security audits of connectors

## 🚀 Future Enhancements

### Planned Features
- **Webhook triggers**: Real-time data ingestion
- **Data quality checks**: Automated validation
- **Alerting system**: Slack/email notifications
- **Retry logic**: Automatic failure recovery
- **Cost optimization**: Dynamic scaling
- **Multi-environment**: Production deployment

### Extensibility
The system is designed to be easily extensible:
- Add new data sources by updating configuration
- Modify schedules without code changes
- Scale parallel execution as needed
- Support for different environments
- Integration with monitoring systems