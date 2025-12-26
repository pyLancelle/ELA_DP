#!/bin/bash

# Configuration
PROJECT_ID="polar-scene-465223-f7"
REGION="europe-west1"

# Couleurs pour les logs
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Fonction pour logger
log() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} ✓ $1"
}

log_error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} ✗ $1"
}

log_section() {
    echo -e "\n${YELLOW}========================================${NC}"
    echo -e "${YELLOW}$1${NC}"
    echo -e "${YELLOW}========================================${NC}\n"
}

# Fonction pour exécuter un Cloud Run job
execute_job() {
    local job_name=$1
    log "Lancement de ${job_name}..."

    if gcloud run jobs execute "${job_name}" \
        --region="${REGION}" \
        --project="${PROJECT_ID}" \
        --quiet 2>&1; then
        log_success "${job_name} démarré avec succès"
        return 0
    else
        log_error "Échec du lancement de ${job_name}"
        return 1
    fi
}

# Fonction pour exécuter plusieurs jobs en parallèle
execute_jobs_parallel() {
    local jobs_string="$1"
    local pids=()

    # Convertir la chaîne en tableau
    IFS='|' read -ra jobs <<< "$jobs_string"

    for job in "${jobs[@]}"; do
        execute_job "${job}" &
        pids+=($!)
    done

    # Attendre que tous les jobs soient lancés
    for pid in "${pids[@]}"; do
        wait $pid
    done
}

# Fonction pour exécuter plusieurs jobs en série
execute_jobs_serial() {
    local jobs_string="$1"

    # Convertir la chaîne en tableau
    IFS='|' read -ra jobs <<< "$jobs_string"

    for job in "${jobs[@]}"; do
        execute_job "${job}"
        sleep 2  # Petit délai entre chaque job
    done
}

# Définition des groupes de jobs (format pipe-separated pour compatibilité bash 3.x)
GARMIN_FETCH_JOBS="garmin-activities|garmin-activity-details|garmin-activity-exercise-sets|garmin-activity-hr-zones|garmin-activity-splits|garmin-activity-weather|garmin-all-day-events|garmin-body-battery|garmin-body-composition|garmin-endurance-score|garmin-floors|garmin-heart-rate|garmin-hill-score|garmin-hrv|garmin-intensity-minutes|garmin-max-metrics|garmin-race-predictions|garmin-respiration|garmin-rhr-daily|garmin-sleep|garmin-spo2|garmin-stats-and-body|garmin-steps|garmin-stress|garmin-training-readiness|garmin-training-status|garmin-user-summary|garmin-weight"

SPOTIFY_FETCH_JOBS="spotify-recently-played|spotify-saved-albums|spotify-saved-tracks|spotify-top-artists"

INGESTION_JOBS="garmin-ingest|spotify-ingest"

DBT_JOBS="dbt-run-garmin|dbt-run-spotify"

# Début de l'exécution
log_section "Démarrage de l'orchestration des Cloud Run Jobs"

# 1. FETCH GARMIN
GARMIN_COUNT=$(echo "$GARMIN_FETCH_JOBS" | tr '|' '\n' | wc -l)
log_section "1/4 - Lancement des jobs FETCH GARMIN ($GARMIN_COUNT jobs)"
execute_jobs_parallel "$GARMIN_FETCH_JOBS"
log_success "Tous les jobs Garmin fetch ont été lancés"

# Attendre un peu avant de passer à l'étape suivante
log "Pause de 30 secondes avant de lancer les jobs Spotify..."
sleep 30

# 2. FETCH SPOTIFY
SPOTIFY_COUNT=$(echo "$SPOTIFY_FETCH_JOBS" | tr '|' '\n' | wc -l)
log_section "2/4 - Lancement des jobs FETCH SPOTIFY ($SPOTIFY_COUNT jobs)"
execute_jobs_parallel "$SPOTIFY_FETCH_JOBS"
log_success "Tous les jobs Spotify fetch ont été lancés"

# Attendre que les fetch se terminent avant de lancer les ingestions
log "Pause de 5 minutes pour laisser les fetch se terminer..."
sleep 300

# 3. INGESTION
INGESTION_COUNT=$(echo "$INGESTION_JOBS" | tr '|' '\n' | wc -l)
log_section "3/4 - Lancement des jobs INGESTION ($INGESTION_COUNT jobs)"
execute_jobs_serial "$INGESTION_JOBS"
log_success "Tous les jobs d'ingestion ont été lancés"

# Attendre que les ingestions se terminent avant de lancer DBT
log "Pause de 5 minutes pour laisser les ingestions se terminer..."
sleep 300

# 4. DBT
DBT_COUNT=$(echo "$DBT_JOBS" | tr '|' '\n' | wc -l)
log_section "4/4 - Lancement des jobs DBT ($DBT_COUNT jobs)"
execute_jobs_serial "$DBT_JOBS"
log_success "Tous les jobs DBT ont été lancés"

# Fin
log_section "Orchestration terminée"
log_success "Tous les jobs ont été lancés avec succès!"
echo ""
log "Pour suivre l'exécution des jobs:"
echo "  gcloud run jobs executions list --region=${REGION} --project=${PROJECT_ID}"
echo ""
log "Pour voir les logs d'un job spécifique:"
echo "  gcloud run jobs executions describe EXECUTION_NAME --region=${REGION} --project=${PROJECT_ID}"
