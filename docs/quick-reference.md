# Chalk and Duster - Quick Reference Card

## 🚀 Essential Commands

### Start Platform
```bash
docker compose up -d
```

### Stop Platform
```bash
docker compose down
```

### View Logs
```bash
docker logs chalkandduster-api --tail 50 -f
```

---

## 🌐 Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| **API Docs** | http://localhost:8000/docs | - |
| **Grafana** | http://localhost:3000 | admin / admin |
| **Prometheus** | http://localhost:9090 | - |
| **API Health** | http://localhost:8000/health | - |

---

## 📡 Core API Endpoints

### Tenants (Organizations)
```bash
# Create
POST /api/v1/tenants
{"name": "Company", "slug": "company"}

# Get
GET /api/v1/tenants/{id}
```

### Connections (Snowflake)
```bash
# Create
POST /api/v1/connections
{"tenant_id": "...", "name": "...", "account": "...", ...}

# Test
POST /api/v1/connections/{id}/test
```

### Datasets (Tables)
```bash
# Create with YAML
POST /api/v1/datasets
{"tenant_id": "...", "connection_id": "...", "quality_yaml": "...", ...}

# Trigger check
POST /api/v1/datasets/{id}/trigger
{"check_type": "quality|drift|both"}
```

### Runs (Results)
```bash
# List runs
GET /api/v1/runs?dataset_id={id}

# Get run details
GET /api/v1/runs/{run_id}
```

---

## 📝 YAML Quick Templates

### Quality Checks
```yaml
checks:
  table_name:
    - row_count > 0
    - missing_count(id) = 0
    - duplicate_count(id) = 0
    - freshness(updated_at) < 1d
```

### Drift Monitors
```yaml
monitors:
  - name: schema
    type: schema
    threshold: 0
  - name: volume
    type: volume
    threshold: 3.0
```

---

## 🔍 Check Types

| Check | Syntax | Description |
|-------|--------|-------------|
| Row Count | `row_count > 0` | Table not empty |
| Missing | `missing_count(col) = 0` | No nulls |
| Duplicates | `duplicate_count(col) = 0` | No duplicates |
| Freshness | `freshness(col) < 1d` | Data is recent |
| Range | `avg(col) between 10 and 100` | Value in range |
| Valid Values | `values in (col) in (...)` | Allowed values |

---

## 📊 Drift Types

| Type | Description | Threshold |
|------|-------------|-----------|
| `schema` | Column changes | 0 = any change |
| `volume` | Row count changes | Std deviations |
| `distribution` | Value distribution | % shift |

---

## 🤖 LLM Endpoints

```bash
# Generate YAML
POST /api/v1/llm/generate-yaml
{"table_name": "orders", "columns": ["id", "amount"]}

# Enhance Alert
POST /api/v1/llm/enhance-alert
{"alert_type": "quality_failure", "results": [...]}
```

---

## 🔧 Troubleshooting

| Issue | Solution |
|-------|----------|
| API not starting | `docker logs chalkandduster-api` |
| No Grafana data | Run triggers, check time range |
| LLM timeout | Using tinyllama (CPU mode) |
| Connection failed | Verify Snowflake credentials |

---

## 📈 Key Metrics

- `chalkandduster_quality_checks_total`
- `chalkandduster_drift_detections_total`
- `chalkandduster_drift_detected_total`
- `chalkandduster_llm_requests_total`

