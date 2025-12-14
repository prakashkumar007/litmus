# Litmus

**Data Quality & AIOps Platform**

A self-service, multi-tenant platform for data quality monitoring, drift detection, and LLM-enhanced alerting.

## Features

- **YAML-First Configuration**: Define data quality checks and drift monitors using simple YAML files
- **LLM-Powered YAML Generation**: Natural language to YAML conversion using Ollama
- **Data Quality Checks**: Powered by Soda Core with SodaCL syntax
- **Drift Detection**: Schema, volume, and statistical distribution monitoring
- **LLM-Enhanced Alerts**: Intelligent alert summarization and root cause analysis
- **Multi-Tenant Architecture**: Secure tenant isolation with per-tenant connections
- **Slack Integration**: Rich Block Kit formatted notifications
- **Lambda/Airflow Orchestration**: Scheduled quality checks and drift detection

## Quick Start

```bash
# Copy environment file
cp .env.example .env

# Start all services
docker compose up -d

# View API documentation
open http://localhost:8000/docs
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         CHALK AND DUSTER                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐  │
│   │   LLM           │   │Great Expectation│   │   Ollama/Bedrock│  │
│   │                 │   │   Quality       │   │   Local LLM     │  │
│   └────────┬────────┘   └────────┬────────┘   └────────┬────────┘  │
│            │                     │                     │           │
│   ┌────────┴─────────────────────┴─────────────────────┴────────┐  │
│   │                        Core Engine                          │  │
│   │   - YAML Validation    - Drift Detection                    │  │
│   │   - Alert Enhancement  - Metrics Collection                 │  │
│   └────────┬─────────────────────┬─────────────────────┬────────┘  │
│            │                     │                     │           │
│   ┌────────┴────────┐   ┌────────┴────────┐   ┌────────┴────────┐  │
│   │   Snowflake     │   │   Snowflake     │   │   LocalStack    │  │
│   │   Control Plane │   │   (LocalStack)  │   │   SQS/SNS       │  │
│   └─────────────────┘   └─────────────────┘   └─────────────────┘  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| PostgreSQL | 5432 | Control plane database |
| LocalStack | 4566 | AWS + Snowflake emulation |
| Ollama | 11434 | Local LLM |


## Environment Variables

See `.env.example` for all configuration options.

Key settings:
- `SNOWFLAKE_USE_LOCALSTACK=true` - Use LocalStack Snowflake emulator
- `OLLAMA_BASE_URL=http://localhost:11434` - Ollama endpoint
- `DATABASE_URL` - PostgreSQL connection string

## Development

```bash
# Install dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run linting
ruff check src/

# Run type checking
mypy src/
```

## License

MIT

