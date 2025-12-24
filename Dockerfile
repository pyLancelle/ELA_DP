# Use Python slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies for building Python packages
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy dependency files
COPY pyproject.toml .
COPY uv.lock .

# Install dependencies with uv
RUN uv sync --frozen --no-dev

# Copy source code
COPY src/ ./src/

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV SPOTIFY_ENDPOINT=saved_tracks
ENV SPOTIFY_LIMIT=50

# Use uv to run the command
CMD uv run python -m src.connectors.spotify.spotify_fetch ${SPOTIFY_ENDPOINT} --limit ${SPOTIFY_LIMIT}