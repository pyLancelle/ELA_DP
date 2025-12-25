#!/bin/bash

PROJECT_ID="polar-scene-465223-f7"
REGION="europe-west1"
IMAGE="europe-west1-docker.pkg.dev/polar-scene-465223-f7/ela-dataplatform/fetcher:latest"

echo "=========================================="
echo "🔄 Creating dbt Cloud Run Jobs"
echo "=========================================="
echo ""

# Function to create a dbt job
create_dbt_job() {
    local job_name=$1
    local dbt_command=${2:-run}
    local dbt_select=${3:-}
    local dbt_target=${4:-dev}
    local memory=${5:-2Gi}
    local cpu=${6:-2}
    local timeout=${7:-20m}
    
    echo "Creating dbt job: $job_name"
    echo "  Command: $dbt_command"
    echo "  Select: $dbt_select"
    echo "  Target: $dbt_target"
    
    local env_vars="MODE=dbt,DBT_COMMAND=$dbt_command,DBT_TARGET=$dbt_target"
    
    if [ -n "$dbt_select" ]; then
        env_vars="${env_vars},DBT_SELECT=$dbt_select"
    fi
    
    gcloud run jobs create $job_name \
        --image=$IMAGE \
        --region=$REGION \
        --set-env-vars="$env_vars" \
        --max-retries=0 \
        --task-timeout=$timeout \
        --memory=$memory \
        --cpu=$cpu \
        2>/dev/null
    
    if [ $? -eq 0 ]; then
        echo "✅ Created: $job_name"
    else
        echo "⚠️  Trying to update..."
        gcloud run jobs update $job_name \
            --image=$IMAGE \
            --region=$REGION \
            --set-env-vars="$env_vars" \
            2>/dev/null
        
        if [ $? -eq 0 ]; then
            echo "✅ Updated: $job_name"
        else
            echo "❌ Failed: $job_name"
        fi
    fi
    
    echo ""
}

# ===========================================
# DBT JOBS - BY LAYER
# ===========================================
echo "📊 Creating dbt jobs by layer..."
echo ""

create_dbt_job "dbt-run-bronze" "run" "tag:bronze" "dev" "2Gi" "2" "20m"
create_dbt_job "dbt-run-silver" "run" "tag:silver" "dev" "2Gi" "2" "20m"
create_dbt_job "dbt-run-gold" "run" "tag:gold" "dev" "2Gi" "2" "20m"

# ===========================================
# DBT JOBS - BY SOURCE
# ===========================================
echo "📱 Creating dbt jobs by source..."
echo ""

create_dbt_job "dbt-run-spotify" "run" "spotify" "dev" "1Gi" "1" "15m"
create_dbt_job "dbt-run-garmin" "run" "garmin" "dev" "2Gi" "2" "20m"
create_dbt_job "dbt-run-chess" "run" "chess" "dev" "1Gi" "1" "15m"

# ===========================================
# DBT JOBS - SPECIAL COMMANDS
# ===========================================
echo "🧪 Creating dbt special jobs..."
echo ""

create_dbt_job "dbt-test" "test" "" "dev" "1Gi" "1" "10m"
create_dbt_job "dbt-build" "build" "" "dev" "2Gi" "2" "30m"
create_dbt_job "dbt-run-all" "run" "" "dev" "2Gi" "2" "30m"

# ===========================================
# SUMMARY
# ===========================================
echo ""
echo "=========================================="
echo "✅ dbt Jobs Creation Complete!"
echo "=========================================="
echo ""
echo "Created jobs:"
echo "  - 3 layer jobs (bronze, silver, gold)"
echo "  - 3 source jobs (spotify, garmin, chess)"
echo "  - 3 special jobs (test, build, run-all)"
echo ""
echo "Total: 9 dbt jobs"
echo ""
echo "🔗 View all jobs:"
echo "https://console.cloud.google.com/run/jobs?project=${PROJECT_ID}"
echo ""
echo "▶️  Execute a job:"
echo "gcloud run jobs execute dbt-run-bronze --region=${REGION} --wait"
echo ""
echo "=========================================="