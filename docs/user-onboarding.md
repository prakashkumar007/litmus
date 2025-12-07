# Chalk and Duster - User Onboarding Guide

## Welcome to Chalk and Duster! 🎯

Chalk and Duster (CD_) is a self-service Data Quality, Drift Detection, and AIOps platform that helps you monitor your Snowflake data with simple YAML configurations.

---

## Quick Start (5 Minutes)

### Step 1: Create Your Organization (Tenant)

Every organization gets their own isolated workspace.

```bash
curl -X POST http://localhost:8000/api/v1/tenants \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Your Company Name",
    "slug": "your-company",
    "description": "Data quality monitoring for our data warehouse"
  }'
```

**Save the `id` from the response** - you'll need it for the next steps.

---

### Step 2: Connect Your Snowflake Database

```bash
curl -X POST http://localhost:8000/api/v1/connections \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "YOUR_TENANT_ID",
    "name": "Production Snowflake",
    "account": "your-account.snowflakecomputing.com",
    "warehouse": "COMPUTE_WH",
    "database_name": "PROD_DB",
    "schema_name": "PUBLIC",
    "role_name": "DATA_READER",
    "user": "service_account",
    "password": "your_password"
  }'
```

**Save the `id`** - this is your connection ID.

---

### Step 3: Register a Dataset with Quality Rules

```bash
curl -X POST http://localhost:8000/api/v1/datasets \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "YOUR_TENANT_ID",
    "connection_id": "YOUR_CONNECTION_ID",
    "name": "Customers Table",
    "database_name": "PROD_DB",
    "schema_name": "PUBLIC",
    "table_name": "CUSTOMERS",
    "quality_yaml": "checks:\n  CUSTOMERS:\n    - row_count > 0\n    - missing_count(customer_id) = 0\n    - missing_count(email) = 0\n    - duplicate_count(customer_id) = 0",
    "drift_yaml": "monitors:\n  - name: schema_monitor\n    type: schema\n    threshold: 0\n  - name: volume_monitor\n    type: volume\n    threshold: 3.0",
    "quality_schedule": "0 6 * * *",
    "tags": ["production", "customers", "critical"]
  }'
```

---

### Step 4: Run Your First Quality Check

```bash
curl -X POST http://localhost:8000/api/v1/datasets/YOUR_DATASET_ID/trigger \
  -H "Content-Type: application/json" \
  -d '{"check_type": "both"}'
```

---

### Step 5: View Results

```bash
# List all runs for your dataset
curl "http://localhost:8000/api/v1/runs?dataset_id=YOUR_DATASET_ID"

# Or visit Grafana dashboards
open http://localhost:3000/d/chalkandduster-quality/quality-overview
# Login: admin / admin
```

---

## Writing Quality YAML (SodaCL Format)

### Basic Checks

```yaml
checks:
  your_table_name:
    # Row count validation
    - row_count > 0
    - row_count between 1000 and 1000000
    
    # Missing value checks
    - missing_count(column_name) = 0
    - missing_percent(column_name) < 5
    
    # Duplicate checks
    - duplicate_count(primary_key) = 0
    
    # Freshness checks
    - freshness(updated_at) < 1d
    
    # Value range checks
    - avg(amount) between 10 and 1000
    - min(quantity) >= 0
    - max(price) <= 10000
```

### Valid Values Check

```yaml
checks:
  orders:
    - values in (status) in ('pending', 'shipped', 'delivered', 'cancelled')
    - values in (country_code) in ('US', 'CA', 'UK', 'DE', 'FR')
```

---

## Writing Drift YAML

### Monitor Types

```yaml
monitors:
  # Schema drift - detect column changes
  - name: schema_monitor
    type: schema
    threshold: 0  # Alert on any schema change

  # Volume drift - detect unusual row count changes
  - name: volume_monitor
    type: volume
    threshold: 3.0  # Alert if >3 standard deviations from normal

  # Distribution drift - detect value distribution changes
  - name: category_distribution
    type: distribution
    column: status
    threshold: 0.25  # Alert if distribution shifts >25%
```

---

## LLM-Assisted YAML Generation

Don't want to write YAML manually? Let AI help you!

```bash
curl -X POST http://localhost:8000/api/v1/llm/generate-yaml \
  -H "Content-Type: application/json" \
  -d '{
    "table_name": "orders",
    "columns": ["order_id", "customer_id", "amount", "status", "created_at"],
    "description": "E-commerce orders table with transaction data"
  }'
```

---

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/tenants` | POST | Create organization |
| `/api/v1/tenants/{id}` | GET | Get organization details |
| `/api/v1/connections` | POST | Add Snowflake connection |
| `/api/v1/connections/{id}/test` | POST | Test connection |
| `/api/v1/datasets` | POST | Register dataset |
| `/api/v1/datasets/{id}` | GET/PATCH | Get/Update dataset |
| `/api/v1/datasets/{id}/trigger` | POST | Run quality/drift check |
| `/api/v1/runs` | GET | List check runs |
| `/api/v1/runs/{id}` | GET | Get run details |
| `/api/v1/llm/generate-yaml` | POST | Generate YAML with AI |
| `/api/v1/llm/enhance-alert` | POST | Enhance alerts with AI |

**Full API Documentation**: http://localhost:8000/docs

---

## Dashboards & Monitoring

### Grafana Dashboards

| Dashboard | URL | Description |
|-----------|-----|-------------|
| Quality Overview | http://localhost:3000/d/chalkandduster-quality | Quality checks & drift metrics |

**Login**: admin / admin

### Key Metrics

- **Quality Checks (24h)**: Total quality check executions
- **Failed Checks (24h)**: Number of failed checks
- **Drift Detection Runs (24h)**: Total drift detection runs
- **Drift Events Detected (24h)**: Number of drifts found
- **Drift by Type**: Breakdown by schema/volume/distribution

---

## Common Use Cases

### Healthcare Data Compliance
```yaml
checks:
  patients:
    - missing_count(patient_id) = 0
    - missing_count(ssn) = 0
    - duplicate_count(patient_id) = 0
    - values in (status) in ('active', 'inactive', 'deceased')
```

### E-Commerce Order Validation
```yaml
checks:
  orders:
    - row_count > 0
    - freshness(order_date) < 1d
    - min(order_amount) >= 0
    - max(order_amount) <= 100000
    - missing_count(customer_id) = 0
```

### Financial Data Integrity
```yaml
checks:
  transactions:
    - duplicate_count(transaction_id) = 0
    - sum(debit) = sum(credit)
    - missing_count(account_id) = 0
    - freshness(transaction_time) < 1h
```

---

## Troubleshooting

### Check Not Running?
1. Verify dataset has `quality_yaml` or `drift_yaml` configured
2. Check the run status: `GET /api/v1/runs?dataset_id=YOUR_ID`
3. View API logs: `docker logs chalkandduster-api --tail 50`

### Connection Failed?
1. Test connection: `POST /api/v1/connections/{id}/test`
2. Verify Snowflake credentials and network access
3. Check warehouse is running and accessible

### Need Help?
- **API Docs**: http://localhost:8000/docs
- **Grafana**: http://localhost:3000
- **Prometheus**: http://localhost:9090

---

## Next Steps

1. ✅ Set up your first dataset with quality rules
2. 📊 Configure Grafana alerts for failed checks
3. 💬 Enable Slack notifications for your tenant
4. ⏰ Set up scheduled checks with cron expressions
5. 🤖 Use LLM to generate YAML for new tables

