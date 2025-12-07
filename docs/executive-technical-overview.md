# Chalk and Duster - Executive Technical Overview

## Platform Summary

**Chalk and Duster (CD_)** is an open-source, self-service Data Quality, Drift Detection, and AIOps platform designed for modern data teams.

### Value Proposition

| Problem | Solution |
|---------|----------|
| Data quality issues discovered too late | Proactive monitoring with automated checks |
| Schema changes break downstream pipelines | Real-time drift detection and alerting |
| Complex setup for data monitoring tools | YAML-first configuration, no coding required |
| Siloed monitoring across teams | Multi-tenant architecture with isolation |
| Alert fatigue from unclear messages | LLM-enhanced alerting with context |

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CHALK AND DUSTER PLATFORM                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                 │
│  │   Tenants    │────▶│  Connections │────▶│   Datasets   │                 │
│  │ (Companies)  │     │ (Snowflake)  │     │  (Tables)    │                 │
│  └──────────────┘     └──────────────┘     └──────────────┘                 │
│                                                   │                          │
│                              ┌────────────────────┴────────────────────┐     │
│                              ▼                                         ▼     │
│                    ┌──────────────────┐                    ┌────────────────┐│
│                    │  Quality Checks  │                    │ Drift Detection││
│                    │   (Soda Core)    │                    │   Monitoring   ││
│                    └────────┬─────────┘                    └───────┬────────┘│
│                             │                                      │         │
│                             ▼                                      ▼         │
│                    ┌──────────────────────────────────────────────────┐     │
│                    │              Observability Layer                  │     │
│                    │   Prometheus │ Grafana │ Loki │ Alertmanager     │     │
│                    └──────────────────────────────────────────────────┘     │
│                                          │                                   │
│                              ┌───────────┴───────────┐                      │
│                              ▼                       ▼                       │
│                    ┌──────────────┐         ┌──────────────┐                │
│                    │    Slack     │         │   LLM/AI     │                │
│                    │   Alerts     │         │ Enhancement  │                │
│                    └──────────────┘         └──────────────┘                │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

### Core Technologies

| Component | Technology | Purpose |
|-----------|------------|---------|
| **API Server** | FastAPI (Python 3.11) | High-performance async REST API |
| **Database** | PostgreSQL 15 | Metadata storage with JSONB support |
| **Data Quality** | Soda Core (SodaCL) | YAML-native quality checks |
| **LLM Integration** | Ollama (Local) | Privacy-first AI assistance |
| **Metrics** | Prometheus | Time-series metrics collection |
| **Dashboards** | Grafana | Real-time visualization |
| **Logging** | Loki + Promtail | Centralized log aggregation |
| **Containers** | Docker Compose | Unified deployment |

### Data Sources (Current)

| Source | Status | Description |
|--------|--------|-------------|
| **Snowflake** | ✅ Supported | Primary data warehouse integration |
| **LocalStack** | ✅ Dev Mode | Snowflake emulation for development |

---

## Key Features

### 1. YAML-First Configuration

No coding required. Simple, readable YAML for all configurations:

```yaml
checks:
  customers:
    - row_count > 0
    - missing_count(email) = 0
    - duplicate_count(customer_id) = 0
    - freshness(updated_at) < 1d
```

### 2. Multi-Tenant Architecture

- Complete data isolation between organizations
- Tenant-specific connections and configurations
- Role-based access control (planned)

### 3. LLM-Enhanced Operations

- **Auto-generate YAML**: Describe your table, get quality rules
- **Smart Alerts**: Context-aware alert messages
- **Drift Explanation**: Human-readable drift analysis
- **Privacy-First**: Local LLM, no data leaves your infrastructure

### 4. Comprehensive Drift Detection

| Drift Type | Description | Use Case |
|------------|-------------|----------|
| **Schema** | Column additions, removals, type changes | ETL pipeline protection |
| **Volume** | Unusual row count changes | Data load validation |
| **Distribution** | Value distribution shifts | Business anomaly detection |

### 5. Real-Time Observability

- Pre-configured Grafana dashboards
- Prometheus metrics for all operations
- Centralized logging with Loki
- Alert management with Alertmanager

---

## Security & Compliance

### Data Security

| Feature | Implementation |
|---------|----------------|
| **Credential Storage** | AWS Secrets Manager integration (planned) |
| **Tenant Isolation** | Database-level row isolation |
| **API Security** | JWT authentication (planned) |
| **Encryption** | TLS for all connections |
| **Audit Logging** | Complete audit trail of all operations |

### Privacy-First AI

- **Local LLM**: Ollama runs entirely on-premises
- **No External Calls**: Data never leaves your infrastructure
- **Configurable Models**: Choose model size vs. performance tradeoff

---

## Deployment Options

### Current (MVP)

```bash
# Single command deployment
docker compose up -d

# Services automatically start:
# - API Server (port 8000)
# - PostgreSQL (port 5432)
# - Prometheus (port 9090)
# - Grafana (port 3000)
# - Ollama LLM (port 11434)
```

### Production Roadmap

| Environment | Technology | Status |
|-------------|------------|--------|
| **Local/Dev** | Docker Compose | ✅ Available |
| **Kubernetes** | Helm Charts | 🔄 Planned |
| **AWS** | ECS/Fargate | 🔄 Planned |
| **Managed** | SaaS Offering | 🔄 Future |

---

## API Design

### RESTful Architecture

```
/api/v1/
├── tenants/          # Organization management
├── connections/      # Database connections
├── datasets/         # Table configurations
│   └── {id}/trigger  # Manual check execution
├── runs/             # Check execution history
├── llm/              # AI-assisted operations
│   ├── generate-yaml
│   ├── enhance-alert
│   └── explain-drift
└── health            # Service health checks
```

### OpenAPI Documentation

- **Interactive Docs**: http://localhost:8000/docs
- **OpenAPI Spec**: http://localhost:8000/openapi.json

---

## Metrics & KPIs

### Platform Metrics

| Metric | Description |
|--------|-------------|
| `chalkandduster_quality_checks_total` | Total quality check runs |
| `chalkandduster_quality_checks_failed_total` | Failed quality checks |
| `chalkandduster_drift_detections_total` | Drift detection runs |
| `chalkandduster_drift_detected_total` | Drift events by type |
| `chalkandduster_llm_requests_total` | LLM API usage |
| `chalkandduster_api_requests_total` | API request volume |

### Business KPIs

| KPI | Calculation | Target |
|-----|-------------|--------|
| **Data Quality Score** | Passed checks / Total checks | > 95% |
| **Mean Time to Detection** | Avg time from issue to alert | < 15 min |
| **Drift Detection Rate** | Drifts detected / Drifts occurred | > 99% |
| **Alert Accuracy** | True positives / Total alerts | > 90% |

---

## Competitive Comparison

| Feature | Chalk & Duster | Monte Carlo | Great Expectations | Soda |
|---------|----------------|-------------|---------------------|------|
| **Open Source** | ✅ | ❌ | ✅ | ⚠️ Partial |
| **Self-Hosted** | ✅ | ❌ | ✅ | ✅ |
| **YAML Config** | ✅ | ❌ | ❌ | ✅ |
| **Multi-Tenant** | ✅ | ✅ | ❌ | ❌ |
| **LLM Integration** | ✅ | ✅ | ❌ | ❌ |
| **Local LLM** | ✅ | ❌ | ❌ | ❌ |
| **Drift Detection** | ✅ | ✅ | ❌ | ⚠️ Limited |
| **Built-in Dashboards** | ✅ | ✅ | ❌ | ⚠️ Cloud only |

---

## Roadmap

### Phase 1: MVP ✅ (Current)
- [x] Multi-tenant API
- [x] Snowflake connectivity
- [x] Quality checks (SodaCL)
- [x] Drift detection (schema, volume, distribution)
- [x] LLM-assisted YAML generation
- [x] Prometheus metrics
- [x] Grafana dashboards
- [x] Manual trigger endpoint

### Phase 2: Production Ready (Q1 2024)
- [ ] JWT authentication
- [ ] AWS Secrets Manager integration
- [ ] Airflow DAG orchestration
- [ ] Slack notifications
- [ ] Email alerts
- [ ] Kubernetes Helm charts

### Phase 3: Enterprise (Q2 2024)
- [ ] Role-based access control (RBAC)
- [ ] SSO integration (SAML/OIDC)
- [ ] Databricks connectivity
- [ ] BigQuery connectivity
- [ ] Data lineage integration
- [ ] Custom check plugins

### Phase 4: Scale (Q3 2024)
- [ ] Distributed execution
- [ ] Horizontal scaling
- [ ] Multi-region deployment
- [ ] SaaS offering

---

## Getting Started

### Prerequisites
- Docker & Docker Compose
- 8GB+ RAM recommended
- LocalStack Pro license (for Snowflake emulation in dev)

### Quick Start

```bash
# Clone repository
git clone https://github.com/your-org/chalkandduster.git
cd chalkandduster

# Set environment variables
export LOCALSTACK_AUTH_TOKEN=your_token

# Start all services
docker compose up -d

# Access services
open http://localhost:8000/docs   # API Documentation
open http://localhost:3000        # Grafana (admin/admin)
```

---

## Contact & Support

| Resource | Link |
|----------|------|
| **API Documentation** | http://localhost:8000/docs |
| **Grafana Dashboards** | http://localhost:3000 |
| **Prometheus Metrics** | http://localhost:9090 |
| **Source Code** | https://github.com/your-org/chalkandduster |

---

*Document Version: 1.0 | Last Updated: December 2024*

