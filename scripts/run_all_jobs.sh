#!/bin/bash

REGION="europe-west1"

# Couleurs
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=========================================="
echo "🚀 Executing ALL Cloud Run Jobs"
echo "=========================================="
echo ""

# Récupérer tous les jobs
JOBS=$(gcloud run jobs list --region=$REGION --format="value(name)")

# Compter
TOTAL=$(echo "$JOBS" | wc -l | tr -d ' ')

echo "Found $TOTAL jobs to execute"
echo ""

COUNTER=0
SUCCESS=0
FAILED=0

# Exécuter chaque job
for JOB in $JOBS; do
    COUNTER=$((COUNTER + 1))
    echo -e "${BLUE}[$COUNTER/$TOTAL] Executing: $JOB${NC}"
    
    # Lancer le job en arrière-plan (pas de --wait pour aller plus vite)
    gcloud run jobs execute $JOB --region=$REGION --async 2>/dev/null
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Started: $JOB${NC}"
        SUCCESS=$((SUCCESS + 1))
    else
        echo -e "${RED}❌ Failed to start: $JOB${NC}"
        FAILED=$((FAILED + 1))
    fi
    
    echo ""
    
    # Petite pause pour ne pas surcharger l'API
    sleep 1
done

echo ""
echo "=========================================="
echo "📊 Execution Summary"
echo "=========================================="
echo -e "${GREEN}✅ Started successfully: $SUCCESS${NC}"
echo -e "${RED}❌ Failed to start: $FAILED${NC}"
echo ""
echo "⏳ Jobs are running in parallel..."
echo "Check status in Cloud Console:"
echo "https://console.cloud.google.com/run/jobs?project=polar-scene-465223-f7"
echo ""
echo "Or check logs:"
echo "gcloud run jobs executions list --region=$REGION"
echo "=========================================="