# ============================================
# Stage 1: Builder - Install dependencies
# ============================================
FROM python:3.11-alpine AS builder

WORKDIR /app

# Install build dependencies for Alpine
RUN apk add --no-cache \
    gcc \
    g++ \
    make \
    musl-dev \
    linux-headers \
    libffi-dev

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy dependency files
COPY pyproject.toml .
COPY uv.lock .

# Install dependencies with uv (will be in .venv)
# Include dbt dependencies for transformations
RUN uv sync --frozen --no-dev --extra dbt

# ============================================
# Stage 2: Runtime - Minimal Alpine image
# ============================================
FROM python:3.11-alpine

WORKDIR /app

# Install only runtime dependencies (no build tools)
RUN apk add --no-cache \
    bash \
    libstdc++ \
    libffi

# Copy uv from builder (for runtime execution)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy installed dependencies from builder
COPY --from=builder /app/.venv /app/.venv

# Copy source code (includes dbt_dataplatform/)
COPY src/ ./src/
COPY api/ ./api/

# Copy entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV PATH="/app/.venv/bin:$PATH"

# Use the entrypoint script
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
