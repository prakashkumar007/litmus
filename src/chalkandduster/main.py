"""
Chalk and Duster - FastAPI Application Entry Point
"""

from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

from chalkandduster.api.routes import datasets, health, llm, runs, tenants, connections, reports, html_reports
from chalkandduster.core.config import settings
from chalkandduster.core.logging import setup_logging
from chalkandduster.db.postgres.session import init_db
# Import metrics module to register Prometheus metrics
import chalkandduster.observability.metrics  # noqa: F401

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    # Startup
    setup_logging()
    logger.info("Starting Chalk and Duster", version="0.1.0", env=settings.APP_ENV)
    
    # Initialize database
    await init_db()
    logger.info("Database initialized")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Chalk and Duster")


# Create FastAPI application
app = FastAPI(
    title="Chalk and Duster",
    description="Data Quality & AIOps Platform - YAML-first, LLM-enhanced",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount Prometheus metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# Include API routes
app.include_router(health.router, tags=["Health"])
app.include_router(
    tenants.router,
    prefix=f"{settings.API_PREFIX}/tenants",
    tags=["Tenants"],
)
app.include_router(
    connections.router,
    prefix=f"{settings.API_PREFIX}/connections",
    tags=["Connections"],
)
app.include_router(
    datasets.router,
    prefix=f"{settings.API_PREFIX}/datasets",
    tags=["Datasets"],
)
app.include_router(
    runs.router,
    prefix=f"{settings.API_PREFIX}/runs",
    tags=["Runs"],
)
app.include_router(
    llm.router,
    prefix=f"{settings.API_PREFIX}/llm",
    tags=["LLM"],
)
app.include_router(
    reports.router,
    prefix=f"{settings.API_PREFIX}/reports",
    tags=["Reports"],
)
app.include_router(
    html_reports.router,
    prefix=f"{settings.API_PREFIX}/html-reports",
    tags=["HTML Reports"],
)


def run():
    """Run the application using uvicorn."""
    import uvicorn
    
    uvicorn.run(
        "chalkandduster.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.APP_DEBUG,
    )


if __name__ == "__main__":
    run()

