#!/bin/bash
set -e

# Determine mode (fetch or ingest)
MODE="${MODE:-fetch}"  # Default to fetch for backward compatibility

# Route to appropriate connector based on MODE

if [ "$MODE" = "api" ]; then
    # ========================================
    # API MODE (FastAPI)
    # ========================================
    echo "🚀 API Mode"
    echo "  Port: ${PORT:-8080}"
    echo ""
    
    # Start FastAPI with uvicorn
    exec uvicorn api.main:app \
        --host 0.0.0.0 \
        --port "${PORT:-8080}" \
        --log-level "${LOG_LEVEL:-info}"

elif [ "$MODE" = "ingest" ]; then
    # ========================================
    # INGESTION MODE
    # ========================================
    CMD_ARGS=()

    # Add service if specified
    if [ -n "$SERVICE" ]; then
        CMD_ARGS+=(--service "$SERVICE")
    fi

    # Add environment (required for ingestion)
    if [ -n "$ENV" ]; then
        CMD_ARGS+=(--env "$ENV")
    fi

    # Add data types if specified
    if [ -n "$DATA_TYPES" ]; then
        CMD_ARGS+=(--data-types "$DATA_TYPES")
    fi

    # Add optional parameters
    if [ -n "$DRY_RUN" ] && [ "$DRY_RUN" = "true" ]; then
        CMD_ARGS+=(--dry-run)
    fi

    if [ -n "$LOG_LEVEL" ]; then
        CMD_ARGS+=(--log-level "$LOG_LEVEL")
    fi

    # If no args were built and no command-line args provided, show help
    if [ ${#CMD_ARGS[@]} -eq 0 ] && [ $# -eq 0 ]; then
        exec uv run python -m src.connectors.ingestor --list-types
    fi

    # Execute with built args or command-line args
    if [ $# -gt 0 ]; then
        # Use command-line args if provided
        exec uv run python -m src.connectors.ingestor "$@"
    else
        # Use environment variables
        exec uv run python -m src.connectors.ingestor "${CMD_ARGS[@]}"
    fi

elif [ "$MODE" = "fetch" ]; then
    # ========================================
    # FETCHING MODE (default)
    # ========================================
    CMD_ARGS=()

    # Add service if specified
    if [ -n "$SERVICE" ]; then
        CMD_ARGS+=(--service "$SERVICE")
    fi

    # Add scope if specified
    if [ -n "$SCOPE" ]; then
        CMD_ARGS+=(--scope "$SCOPE")
    fi

    # Add destination (required for GCS)
    if [ -n "$DESTINATION" ]; then
        CMD_ARGS+=(--destination "$DESTINATION")
    fi

    # Add optional parameters
    if [ -n "$DAYS" ]; then
        CMD_ARGS+=(--days "$DAYS")
    fi

    if [ -n "$LIMIT" ]; then
        CMD_ARGS+=(--limit "$LIMIT")
    fi

    if [ -n "$LOG_LEVEL" ]; then
        CMD_ARGS+=(--log-level "$LOG_LEVEL")
    fi

    # If no args were built and no command-line args provided, show help
    if [ ${#CMD_ARGS[@]} -eq 0 ] && [ $# -eq 0 ]; then
        exec uv run python -m src.connectors.fetcher --list-types
    fi

    # Execute with built args or command-line args
    if [ $# -gt 0 ]; then
        # Use command-line args if provided
        exec uv run python -m src.connectors.fetcher "$@"
    else
        # Use environment variables
        exec uv run python -m src.connectors.fetcher "${CMD_ARGS[@]}"
    fi

elif [ "$MODE" = "dbt" ]; then
    # ========================================
    # DBT MODE
    # ========================================
    echo "🔄 dbt Mode"
    echo "  Command: ${DBT_COMMAND:-run}"
    echo "  Target: ${DBT_TARGET:-dev}"
    echo "  Select: ${DBT_SELECT:-}"
    echo ""

    # Change to dbt project directory
    cd /app/src/dbt_dataplatform

    CMD_ARGS=()

    # dbt command (run, test, build, etc.)
    CMD_ARGS+=("${DBT_COMMAND:-run}")

    # Target environment
    if [ -n "$DBT_TARGET" ]; then
        CMD_ARGS+=(--target "$DBT_TARGET")
    fi

    # Select specific models
    if [ -n "$DBT_SELECT" ]; then
        CMD_ARGS+=(--select "$DBT_SELECT")
    fi

    # Exclude models
    if [ -n "$DBT_EXCLUDE" ]; then
        CMD_ARGS+=(--exclude "$DBT_EXCLUDE")
    fi

    # Full refresh
    if [ -n "$DBT_FULL_REFRESH" ] && [ "$DBT_FULL_REFRESH" = "true" ]; then
        CMD_ARGS+=(--full-refresh)
    fi

    # Execute with built args or command-line args
    if [ $# -gt 0 ]; then
        exec dbt "$@"
    else
        exec dbt "${CMD_ARGS[@]}"
    fi

elif [ "$MODE" = "export" ]; then
    # ========================================
    # EXPORT MODE (GCS Static Files)
    # ========================================
    echo "📤 Export Mode"
    echo "  Type: ${EXPORT_TYPE:-homepage}"
    echo "  Bucket: ${BUCKET:-}"
    echo ""

    CMD_ARGS=()

    # Add bucket if specified
    if [ -n "$BUCKET" ]; then
        CMD_ARGS+=(--bucket "$BUCKET")
    fi

    # Add dry-run if specified
    if [ -n "$DRY_RUN" ] && [ "$DRY_RUN" = "true" ]; then
        CMD_ARGS+=(--dry-run)
    fi

    # Route to appropriate exporter based on EXPORT_TYPE
    EXPORT_TYPE="${EXPORT_TYPE:-homepage}"

    # Add periods if specified (for music export)
    if [ -n "$PERIODS" ]; then
        CMD_ARGS+=(--periods "$PERIODS")
    fi

    # Add skip-details if specified (for activities export)
    if [ -n "$SKIP_DETAILS" ] && [ "$SKIP_DETAILS" = "true" ]; then
        CMD_ARGS+=(--skip-details)
    fi

    # Add limit if specified (for activities export)
    if [ -n "$LIMIT" ]; then
        CMD_ARGS+=(--limit "$LIMIT")
    fi

    # Add scope if specified (for all export)
    if [ -n "$SCOPE" ]; then
        CMD_ARGS+=(--scope "$SCOPE")
    fi

    case "$EXPORT_TYPE" in
        homepage)
            exec uv run python -m src.connectors.exporter.homepage "${CMD_ARGS[@]}"
            ;;
        music)
            exec uv run python -m src.connectors.exporter.music "${CMD_ARGS[@]}"
            ;;
        activities)
            exec uv run python -m src.connectors.exporter.activities "${CMD_ARGS[@]}"
            ;;
        all)
            exec uv run python -m src.connectors.exporter.all "${CMD_ARGS[@]}"
            ;;
        *)
            echo "Error: Unknown EXPORT_TYPE='$EXPORT_TYPE'. Valid values: 'homepage', 'music', 'activities', 'all'"
            exit 1
            ;;
    esac

else
    echo "Error: Unknown MODE='$MODE'. Valid values: 'fetch', 'ingest', 'dbt', 'api', 'export'"
    exit 1
fi
