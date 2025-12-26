#!/bin/bash

# Configuration
PROJECT_ID="polar-scene-465223-f7"
REGION="europe-west1"
IMAGE_REGISTRY="europe-west1-docker.pkg.dev/polar-scene-465223-f7/ela-dataplatform"
BUCKET="gs://ela-dp-dev"

# Couleurs
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=========================================="
echo "🚀 Creating Cloud Run Jobs"
echo "=========================================="
echo ""

# Function to create a job
create_job() {
    local service=$1
    local scope=$2
    local days=${3:-7}
    local limit=${4:-50}
    
    # Convertir underscores en tirets pour le nom du job (convention Cloud Run)
    local job_name="${service}-${scope//_/-}"
    local image="${IMAGE_REGISTRY}/fetcher:latest"
    local destination="${BUCKET}/${service}/landing/"
    
    echo -e "${BLUE}Creating job: ${job_name}${NC}"
    
    # Variables d'environnement (le script docker-entrypoint.sh les convertira)
    local env_vars="SERVICE=${service},SCOPE=${scope},DESTINATION=${destination},DAYS=${days},LIMIT=${limit},LOG_LEVEL=INFO"
    
    gcloud run jobs create $job_name \
        --image=$image \
        --region=$REGION \
        --set-env-vars="${env_vars}" \
        --set-secrets=SPOTIFY_CLIENT_ID=SPOTIFY_CLIENT_ID:latest,SPOTIFY_CLIENT_SECRET=SPOTIFY_CLIENT_SECRET:latest,SPOTIFY_REFRESH_TOKEN=SPOTIFY_REFRESH_TOKEN:latest,SPOTIFY_REDIRECT_URI=SPOTIFY_REDIRECT_URI:latest,GARMIN_USERNAME=GARMIN_USERNAME:latest,GARMIN_PASSWORD=GARMIN_PASSWORD:latest \
        --max-retries=0 \
        --task-timeout=10m \
        --memory=512Mi \
        --cpu=1
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Created: ${job_name}${NC}"
        echo -e "   ${BLUE}Destination: ${destination}${NC}"
    else
        echo -e "${YELLOW}⚠️  Job ${job_name} might already exist, trying to update...${NC}"
        
        gcloud run jobs update $job_name \
            --image=$image \
            --region=$REGION \
            --set-env-vars="${env_vars}" \
            --set-secrets=SPOTIFY_CLIENT_ID=SPOTIFY_CLIENT_ID:latest,SPOTIFY_CLIENT_SECRET=SPOTIFY_CLIENT_SECRET:latest,SPOTIFY_REFRESH_TOKEN=SPOTIFY_REFRESH_TOKEN:latest,SPOTIFY_REDIRECT_URI=SPOTIFY_REDIRECT_URI:latest,GARMIN_USERNAME=GARMIN_USERNAME:latest,GARMIN_PASSWORD=GARMIN_PASSWORD:latest
        
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}✅ Updated: ${job_name}${NC}"
            echo -e "   ${BLUE}Destination: ${destination}${NC}"
        else
            echo -e "${YELLOW}❌ Failed: ${job_name}${NC}"
        fi
    fi
    
    echo ""
}

# ===========================================
# SPOTIFY JOBS
# ===========================================
echo -e "${BLUE}=========================================="
echo "📱 Creating Spotify Jobs (4)"
echo -e "==========================================${NC}"
echo ""

create_job "spotify" "saved_tracks" 7 50
create_job "spotify" "saved_albums" 7 50
create_job "spotify" "recently_played" 7 50
create_job "spotify" "top_artists" 7 50

# ===========================================
# GARMIN JOBS
# ===========================================
echo -e "${BLUE}=========================================="
echo "⌚ Creating Garmin Jobs (28)"
echo -e "==========================================${NC}"
echo ""

create_job "garmin" "activities" 3
create_job "garmin" "activity_details" 3
create_job "garmin" "activity_exercise_sets" 3
create_job "garmin" "activity_hr_zones" 3
create_job "garmin" "activity_splits" 3
create_job "garmin" "activity_weather" 3
create_job "garmin" "all_day_events" 3
create_job "garmin" "body_battery" 3
create_job "garmin" "body_composition" 3
create_job "garmin" "endurance_score" 3
create_job "garmin" "floors" 3
create_job "garmin" "heart_rate" 3
create_job "garmin" "hill_score" 3
create_job "garmin" "hrv" 3
create_job "garmin" "intensity_minutes" 3
create_job "garmin" "max_metrics" 3
create_job "garmin" "race_predictions" 3
create_job "garmin" "respiration" 3
create_job "garmin" "rhr_daily" 3
create_job "garmin" "sleep" 3
create_job "garmin" "spo2" 3
create_job "garmin" "stats_and_body" 3
create_job "garmin" "steps" 3
create_job "garmin" "stress" 3
create_job "garmin" "training_readiness" 3
create_job "garmin" "training_status" 3
create_job "garmin" "user_summary" 3
create_job "garmin" "weight" 3

# ===========================================
# SUMMARY
# ===========================================
echo ""
echo -e "${GREEN}=========================================="
echo "✅ Cloud Run Jobs Creation Complete!"
echo -e "==========================================${NC}"
echo ""
echo "Total jobs created/updated: 32"
echo "  - Spotify: 4 jobs"
echo "  - Garmin: 28 jobs"
echo ""
echo "📍 Destination bucket: ${BUCKET}"
echo ""
echo "🔗 View all jobs:"
echo "https://console.cloud.google.com/run/jobs?project=${PROJECT_ID}"
echo ""
echo "▶️  Execute a job manually:"
echo "gcloud run jobs execute JOB_NAME --region=${REGION}"
echo ""
echo "Example:"
echo "gcloud run jobs execute spotify-recently-played --region=${REGION} --wait"
echo ""
echo "=========================================="