# Chalk and Duster

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
- **Airflow Orchestration**: Scheduled quality checks and drift detection

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
│   │   FastAPI       │   │   Soda Core     │   │   Ollama        │  │
│   │   REST API      │   │   Quality       │   │   Local LLM     │  │
│   └────────┬────────┘   └────────┬────────┘   └────────┬────────┘  │
│            │                     │                     │            │
│   ┌────────┴─────────────────────┴─────────────────────┴────────┐  │
│   │                        Core Engine                          │  │
│   │   - YAML Validation    - Drift Detection                    │  │
│   │   - Alert Enhancement  - Metrics Collection                 │  │
│   └────────┬─────────────────────┬─────────────────────┬────────┘  │
│            │                     │                     │            │
│   ┌────────┴────────┐   ┌────────┴────────┐   ┌────────┴────────┐  │
│   │   PostgreSQL    │   │   Snowflake     │   │   LocalStack    │  │
│   │   Control Plane │   │   (LocalStack)  │   │   SQS/SNS       │  │
│   └─────────────────┘   └─────────────────┘   └─────────────────┘  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| API | 8000 | FastAPI REST API |
| PostgreSQL | 5432 | Control plane database |
| LocalStack | 4566 | AWS + Snowflake emulation |
| Ollama | 11434 | Local LLM |
| Grafana | 3000 | Dashboards |
| Prometheus | 9090 | Metrics |
| Loki | 3100 | Logs |

## API Endpoints

- `POST /api/v1/tenants` - Create tenant
- `POST /api/v1/connections` - Add Snowflake connection
- `POST /api/v1/datasets` - Register dataset with YAML
- `POST /api/v1/llm/generate-yaml` - Generate YAML from natural language
- `GET /health` - Health check

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

