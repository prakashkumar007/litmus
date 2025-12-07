# =============================================================================
# CHALK AND DUSTER - Docker Image
# =============================================================================
# Multi-stage build for optimized production image

# Stage 1: Build
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY pyproject.toml README.md ./
RUN pip install --no-cache-dir build && \
    pip wheel --no-cache-dir --wheel-dir /app/wheels -e .

# Stage 2: Runtime
FROM python:3.11-slim as runtime

WORKDIR /app

# Create non-root user
RUN groupadd -r chalkandduster && \
    useradd -r -g chalkandduster chalkandduster

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy wheels and install
COPY --from=builder /app/wheels /app/wheels
RUN pip install --no-cache-dir /app/wheels/*.whl && \
    rm -rf /app/wheels

# Copy application code
COPY src/ /app/src/
COPY alembic.ini /app/
COPY alembic/ /app/alembic/

# Set ownership
RUN chown -R chalkandduster:chalkandduster /app

# Switch to non-root user
USER chalkandduster

# Environment
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run application
CMD ["uvicorn", "chalkandduster.main:app", "--host", "0.0.0.0", "--port", "8000"]

