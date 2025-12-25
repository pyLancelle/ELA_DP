#!/bin/bash
set -e

# Build command from environment variables
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
