# =============================================================================
# CHALK AND DUSTER - Docker Image
# =============================================================================
# Multi-stage build for optimized production image
# Uses Great Expectations for data quality and Evidently for drift detection

# Stage 1: Build
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
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
COPY lambda/ /app/lambda/
COPY scripts/ /app/scripts/
COPY tests/events/ /app/tests/events/

# Create Great Expectations data context directory
RUN mkdir -p /app/great_expectations && \
    chown -R chalkandduster:chalkandduster /app/great_expectations

# Create Evidently reports directory
RUN mkdir -p /app/evidently_reports && \
    chown -R chalkandduster:chalkandduster /app/evidently_reports

# Set ownership
RUN chown -R chalkandduster:chalkandduster /app

# Switch to non-root user
USER chalkandduster

# Environment
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src \
    GE_DATA_CONTEXT_ROOT=/app/great_expectations \
    EVIDENTLY_DRIFT_THRESHOLD=0.1 \
    EVIDENTLY_STATTEST=ks

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run application
CMD ["uvicorn", "chalkandduster.main:app", "--host", "0.0.0.0", "--port", "8000"]

