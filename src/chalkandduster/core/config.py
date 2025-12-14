"""
Chalk and Duster - Application Configuration
"""

from functools import lru_cache
from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )
    
    # -------------------------------------------------------------------------
    # Application
    # -------------------------------------------------------------------------
    APP_ENV: str = Field(default="development")
    APP_DEBUG: bool = Field(default=True)
    APP_SECRET_KEY: str = Field(default="change-me-in-production")
    API_PREFIX: str = Field(default="/api/v1")
    CORS_ORIGINS: List[str] = Field(default=["*"])
    
    # -------------------------------------------------------------------------
    # PostgreSQL
    # -------------------------------------------------------------------------
    POSTGRES_HOST: str = Field(default="localhost")
    POSTGRES_PORT: int = Field(default=5432)
    POSTGRES_DB: str = Field(default="chalkandduster")
    POSTGRES_USER: str = Field(default="chalkandduster")
    POSTGRES_PASSWORD: str = Field(default="chalkandduster")
    DATABASE_URL: str = Field(
        default="postgresql+asyncpg://chalkandduster:chalkandduster@localhost:5432/chalkandduster"
    )
    
    # -------------------------------------------------------------------------
    # Snowflake (supports LocalStack Snowflake Emulator)
    # -------------------------------------------------------------------------
    # LocalStack Snowflake Emulator settings
    SNOWFLAKE_USE_LOCALSTACK: bool = Field(default=True)
    LOCALSTACK_SNOWFLAKE_HOST: str = Field(default="snowflake.localhost.localstack.cloud")
    LOCALSTACK_SNOWFLAKE_PORT: int = Field(default=4566)

    # Snowflake connection settings (works for both LocalStack and real Snowflake)
    SNOWFLAKE_ACCOUNT: str = Field(default="test")
    SNOWFLAKE_USER: str = Field(default="test")
    SNOWFLAKE_PASSWORD: str = Field(default="test")
    SNOWFLAKE_PRIVATE_KEY_PATH: str = Field(default="")
    SNOWFLAKE_PRIVATE_KEY_PASSPHRASE: str = Field(default="")
    SNOWFLAKE_WAREHOUSE: str = Field(default="test_warehouse")
    SNOWFLAKE_DATABASE: str = Field(default="test_db")
    SNOWFLAKE_SCHEMA: str = Field(default="public")
    SNOWFLAKE_ROLE: str = Field(default="test_role")
    
    # -------------------------------------------------------------------------
    # LLM - Ollama
    # -------------------------------------------------------------------------
    LLM_PROVIDER: str = Field(default="ollama")
    LLM_MODEL: str = Field(default="llama3.2")
    OLLAMA_BASE_URL: str = Field(default="http://localhost:11434")
    OLLAMA_MODEL: str = Field(default="llama3.2")
    OLLAMA_TIMEOUT: int = Field(default=120)
    
    # AWS Bedrock (alternative)
    AWS_REGION: str = Field(default="us-east-1")
    BEDROCK_MODEL_ID: str = Field(default="anthropic.claude-3-sonnet-20240229-v1:0")
    
    # -------------------------------------------------------------------------
    # AWS / LocalStack
    # -------------------------------------------------------------------------
    AWS_ENDPOINT_URL: str | None = Field(default="http://localhost:4566")
    AWS_ACCESS_KEY_ID: str = Field(default="test")
    AWS_SECRET_ACCESS_KEY: str = Field(default="test")
    AWS_DEFAULT_REGION: str = Field(default="us-east-1")
    
    # SQS
    SQS_QUALITY_CHECK_QUEUE: str = Field(default="chalkandduster-quality-checks")
    SQS_DRIFT_CHECK_QUEUE: str = Field(default="chalkandduster-drift-checks")
    SQS_ALERT_QUEUE: str = Field(default="chalkandduster-alerts")
    
    # Secrets Manager
    SECRETS_PREFIX: str = Field(default="chalkandduster/")
    
    # -------------------------------------------------------------------------
    # Slack
    # -------------------------------------------------------------------------
    SLACK_WEBHOOK_URL: str = Field(default="")
    SLACK_DEFAULT_CHANNEL: str = Field(default="#data-quality-alerts")
    
    # -------------------------------------------------------------------------
    # Observability
    # -------------------------------------------------------------------------
    PROMETHEUS_METRICS_PORT: int = Field(default=9090)
    GRAFANA_URL: str = Field(default="http://localhost:3000")
    LOKI_URL: str = Field(default="http://localhost:3100")
    OTEL_EXPORTER_OTLP_ENDPOINT: str = Field(default="http://localhost:4317")
    OTEL_SERVICE_NAME: str = Field(default="chalkandduster")
    
    # -------------------------------------------------------------------------
    # Rate Limiting
    # -------------------------------------------------------------------------
    RATE_LIMIT_LLM_CALLS_PER_HOUR: int = Field(default=100)
    RATE_LIMIT_API_CALLS_PER_MINUTE: int = Field(default=60)
    
    # -------------------------------------------------------------------------
    # HIPAA Compliance
    # -------------------------------------------------------------------------
    HIPAA_MODE: bool = Field(default=True)
    AUDIT_LOG_RETENTION_DAYS: int = Field(default=2190)  # 6 years
    PHI_IN_LOGS: str = Field(default="never")
    SAMPLE_DATA_DISPLAY: str = Field(default="disabled")

    # -------------------------------------------------------------------------
    # Orchestration
    # -------------------------------------------------------------------------
    # Options: "airflow", "lambda"
    ORCHESTRATION_ENGINE: str = Field(default="airflow")

    # Lambda-specific settings
    LAMBDA_QUALITY_FUNCTION_NAME: str = Field(default="chalkandduster-quality-check")
    LAMBDA_DRIFT_FUNCTION_NAME: str = Field(default="chalkandduster-drift-detection")
    LAMBDA_TIMEOUT_SECONDS: int = Field(default=900)  # 15 minutes max
    LAMBDA_MEMORY_MB: int = Field(default=1024)

    # -------------------------------------------------------------------------
    # Data Quality Framework (Great Expectations)
    # -------------------------------------------------------------------------
    GE_DATA_CONTEXT_ROOT: str = Field(default="great_expectations")
    GE_EXPECTATION_SUITE_PREFIX: str = Field(default="chalkandduster")

    # -------------------------------------------------------------------------
    # Drift Detection Framework (Evidently)
    # -------------------------------------------------------------------------
    EVIDENTLY_DRIFT_THRESHOLD: float = Field(default=0.1)
    EVIDENTLY_STATTEST: str = Field(default="ks")  # ks, chisquare, z, wasserstein, psi

    # Baseline Storage (S3)
    BASELINE_BUCKET: str = Field(default="chalkandduster-baselines")
    BASELINE_SAMPLE_SIZE: int = Field(default=10000)

    # Reports Storage (S3)
    REPORTS_BUCKET: str = Field(default="chalkandduster-reports")
    REPORTS_ENABLED: bool = Field(default=True)


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


settings = get_settings()

