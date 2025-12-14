# Local Lambda Testing Guide

This guide explains how to run and test Lambda functions locally for data quality checks (Great Expectations) and drift detection (Evidently) with Snowflake integration.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Local Development Stack                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                │
│   │   Baseline   │     │    Drift     │     │   Quality    │                │
│   │   Lambda     │     │   Lambda     │     │   Lambda     │                │
│   └──────┬───────┘     └──────┬───────┘     └──────┬───────┘                │
│          │                    │                    │                         │
│          ▼                    ▼                    ▼                         │
│   ┌──────────────────────────────────────────────────────────┐              │
│   │                  Snowflake Connector                      │              │
│   │         (LocalStack Snowflake Emulator or Real)          │              │
│   └──────────────────────────────────────────────────────────┘              │
│          │                    │                                              │
│          ▼                    ▼                                              │
│   ┌──────────────┐     ┌──────────────┐                                     │
│   │  LocalStack  │     │  Evidently   │                                     │
│   │  S3 Bucket   │     │  Comparison  │                                     │
│   │  (Baselines) │◄────│              │                                     │
│   └──────────────┘     └──────────────┘                                     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Prerequisites

1. **Docker & Docker Compose** - For running LocalStack and other services
2. **Python 3.11+** - For running Lambda handlers locally
3. **AWS CLI** - For interacting with LocalStack (optional)

## Quick Start

### 1. Start Local Services

```bash
# Start all services (LocalStack, PostgreSQL, etc.)
make docker-up

# This creates:
# - S3 bucket: chalkandduster-baselines
# - S3 bucket: chalkandduster-reports
# - SQS queues for job processing
# - Snowflake emulator (LocalStack Pro feature)
```

### 2. Install Dependencies

```bash
# Install the project with dev dependencies
make dev-install
```

### 3. Test Lambda Functions

```bash
# Test baseline creation
make lambda-baseline

# Test drift detection
make lambda-drift

# Test quality checks
make lambda-quality
```

## Lambda Functions

### Baseline Handler (`lambda/baseline_handler.py`)

Creates and manages baseline data for Evidently drift detection.

**Actions:**
- `create` - Fetch data from Snowflake and save as baseline
- `update` - Replace existing baseline with new data
- `get` - Get baseline metadata
- `delete` - Remove baseline

**Event Payload:**
```json
{
  "action": "create",
  "job_id": "uuid",
  "tenant_id": "uuid",
  "dataset_id": "uuid",
  "table_name": "orders",
  "database": "analytics_db",
  "schema": "public",
  "columns": ["amount", "status", "created_at"],
  "sample_size": 10000,
  "connection_config": {
    "account": "your-account",
    "user": "your-user",
    "password": "your-password",
    "database": "analytics_db",
    "schema": "public",
    "warehouse": "compute_wh"
  }
}
```

### Drift Handler (`lambda/drift_handler.py`)

Detects data drift using Evidently by comparing current data against baseline.

**Event Payload:**
```json
{
  "job_id": "uuid",
  "tenant_id": "uuid",
  "dataset_id": "uuid",
  "table_name": "orders",
  "drift_yaml": "monitors:\n  - name: amount_drift\n    type: distribution\n    column: amount\n    threshold: 0.1\n    stattest: ks",
  "database": "analytics_db",
  "schema": "public",
  "connection_config": {
    "account": "your-account",
    "user": "your-user",
    "password": "your-password",
    "database": "analytics_db",
    "schema": "public",
    "warehouse": "compute_wh"
  }
}
```

### Quality Handler (`lambda/quality_handler.py`)

Runs data quality checks using Great Expectations.

## Snowflake Configuration

### Option 1: LocalStack Snowflake Emulator (Local Development)

LocalStack Pro includes a Snowflake emulator. See [LocalStack Snowflake docs](https://docs.localstack.cloud/snowflake/).

**Environment Variables (already configured in docker-compose.yaml):**

```bash
# .env file
SNOWFLAKE_USE_LOCALSTACK=true
LOCALSTACK_SNOWFLAKE_HOST=localstack
LOCALSTACK_SNOWFLAKE_PORT=4566
SNOWFLAKE_ACCOUNT=test
SNOWFLAKE_USER=test
SNOWFLAKE_PASSWORD=test
SNOWFLAKE_DATABASE=test_db
SNOWFLAKE_SCHEMA=public
SNOWFLAKE_WAREHOUSE=test_warehouse
```

**Test the Snowflake LocalStack connection:**

```bash
# Run the Snowflake integration test
make test-snowflake-localstack

# This will:
# 1. Test raw Snowflake connection
# 2. Create test database, warehouse, and schema
# 3. Test the SnowflakeConnector class
# 4. Create sample tables with test data
```

**Expected output:**

```
============================================================
  Testing Raw Snowflake Connection
============================================================
  ✅ Connected using snowflake.localhost.localstack.cloud!
  ✅ CURRENT_VERSION(): 8.20.0
  ✅ Created warehouse: test_warehouse
  ✅ Created database: test_db
  ✅ Created schema: public

============================================================
  Testing Snowflake Connector
============================================================
  ✅ Connected successfully!
  ✅ CURRENT_VERSION(): 8.20.0
  ✅ CURRENT_WAREHOUSE(): TEST
  ✅ CURRENT_DATABASE(): TEST_DB

============================================================
  Test Summary
============================================================
  🎉 All tests passed! Snowflake LocalStack integration is working.
```

### Option 2: Real Snowflake Account (Integration Testing)

For testing with a real Snowflake account:

```bash
# .env file
SNOWFLAKE_USE_LOCALSTACK=false
SNOWFLAKE_ACCOUNT=abc12345.us-east-1
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_WAREHOUSE=your_warehouse
```

**Security Note:** For production, use AWS Secrets Manager:
```bash
# Store credentials in LocalStack Secrets Manager
awslocal secretsmanager create-secret \
  --name chalkandduster/snowflake \
  --secret-string '{"account":"...","user":"...","password":"..."}'
```

## Baseline Storage (S3)

Baselines are stored in S3 as Parquet files:

```
s3://chalkandduster-baselines/
  └── {tenant_id}/
      └── {dataset_id}/
          ├── baseline.parquet    # The baseline data
          └── metadata.json       # Metadata (columns, row count, etc.)
```

### Viewing Baselines in LocalStack

```bash
# List all baselines
awslocal s3 ls s3://chalkandduster-baselines/ --recursive

# Download a baseline
awslocal s3 cp s3://chalkandduster-baselines/{tenant_id}/{dataset_id}/baseline.parquet ./

# View metadata
awslocal s3 cp s3://chalkandduster-baselines/{tenant_id}/{dataset_id}/metadata.json - | jq
```

## Drift Detection YAML Format (Evidently)

```yaml
monitors:
  # Distribution drift for numeric columns
  - name: amount_distribution_drift
    type: distribution
    column: amount
    threshold: 0.1        # Drift threshold (0-1)
    stattest: ks          # Statistical test: ks, wasserstein, psi

  # Distribution drift for categorical columns
  - name: status_drift
    type: distribution
    column: status
    threshold: 0.1
    stattest: chisquare   # Chi-square test for categorical

  # Dataset-level drift (all columns)
  - name: dataset_drift
    type: dataset
    threshold: 0.3        # % of drifted columns to trigger alert

  # Volume/row count drift
  - name: volume_check
    type: volume
    threshold: 0.5        # 50% change threshold

  # Schema drift detection
  - name: schema_check
    type: schema
```

**Available Statistical Tests:**
| Test | Use Case |
|------|----------|
| `ks` | Kolmogorov-Smirnov - numeric columns |
| `wasserstein` | Wasserstein distance - numeric columns |
| `psi` | Population Stability Index - numeric/categorical |
| `chisquare` | Chi-square - categorical columns |
| `z` | Z-test - numeric columns |

## Quality Check YAML Format (Great Expectations)

```yaml
expectation_suite_name: orders_quality_suite
expectations:
  # Row count validation
  - expectation_type: expect_table_row_count_to_be_between
    kwargs:
      min_value: 1
      max_value: 1000000

  # Not null validation
  - expectation_type: expect_column_values_to_not_be_null
    kwargs:
      column: id

  # Value range validation
  - expectation_type: expect_column_values_to_be_between
    kwargs:
      column: amount
      min_value: 0
      max_value: 100000

  # Unique values
  - expectation_type: expect_column_values_to_be_unique
    kwargs:
      column: order_id

  # Value set validation
  - expectation_type: expect_column_values_to_be_in_set
    kwargs:
      column: status
      value_set: ["pending", "completed", "cancelled"]
```

## Workflow: Setting Up Drift Detection

### Step 1: Create a Baseline

```bash
# Edit the baseline event with your table info
vim tests/events/baseline_event.json

# Run baseline creation
make lambda-baseline
```

### Step 2: Run Drift Detection

```bash
# Edit the drift event with your configuration
vim tests/events/drift_event.json

# Run drift detection
make lambda-drift
```

### Step 3: Interpret Results

```json
{
  "statusCode": 200,
  "body": {
    "status": "completed",
    "results": [
      {
        "monitor_name": "amount_drift",
        "drift_type": "distribution",
        "detected": true,
        "severity": "warning",
        "metric_value": 0.15,
        "threshold": 0.1,
        "message": "Distribution drift detected in column 'amount'"
      }
    ]
  }
}
```

## Troubleshooting

### LocalStack Not Starting

```bash
# Check LocalStack logs
docker compose logs localstack

# Restart LocalStack
docker compose restart localstack
```

### Baseline Not Found

```bash
# Verify baseline exists
awslocal s3 ls s3://chalkandduster-baselines/

# Check if S3 bucket was created
awslocal s3 ls
```

### Snowflake Connection Issues

```bash
# Test Snowflake connectivity (LocalStack) - run inside Docker
make test-snowflake-localstack

# Or manually test the connection
docker compose exec api python -c "
import snowflake.connector as sf
conn = sf.connect(
    user='test',
    password='test',
    account='test',
    host='snowflake.localhost.localstack.cloud',
)
cursor = conn.cursor()
cursor.execute('SELECT CURRENT_VERSION()')
print('Snowflake version:', cursor.fetchone()[0])
cursor.close()
conn.close()
"
```

**Common issues:**
- **DNS resolution**: Inside Docker, use `snowflake.localstack` or `snowflake.localhost.localstack.cloud`
- **Database not found**: Run `make test-snowflake-localstack` first to create the test database
- **Connection refused**: Ensure LocalStack is running with `docker compose ps`

### Import Errors

```bash
# Ensure you're in the project root
cd /path/to/chalkandduster

# Install in development mode
pip install -e ".[dev]"
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `AWS_ENDPOINT_URL` | LocalStack endpoint | `http://localhost:4566` |
| `BASELINE_BUCKET` | S3 bucket for baselines | `chalkandduster-baselines` |
| `BASELINE_SAMPLE_SIZE` | Max rows for baseline | `10000` |
| `EVIDENTLY_DRIFT_THRESHOLD` | Default drift threshold | `0.1` |
| `EVIDENTLY_STATTEST` | Default statistical test | `ks` |
| `GE_DATA_CONTEXT_ROOT` | Great Expectations root | `great_expectations` |
| `SNOWFLAKE_USE_LOCALSTACK` | Use LocalStack Snowflake emulator | `true` |
| `LOCALSTACK_SNOWFLAKE_HOST` | LocalStack host for Snowflake | `localstack` |
| `LOCALSTACK_SNOWFLAKE_PORT` | LocalStack port for Snowflake | `4566` |
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier | `test` |
| `SNOWFLAKE_USER` | Snowflake username | `test` |
| `SNOWFLAKE_PASSWORD` | Snowflake password | `test` |
| `SNOWFLAKE_DATABASE` | Snowflake database name | `test_db` |
| `SNOWFLAKE_SCHEMA` | Snowflake schema name | `public` |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse name | `test_warehouse` |

## Advanced: Deploy to LocalStack Lambda

For more realistic testing, deploy the Lambda functions to LocalStack:

```bash
# Deploy all functions
python scripts/lambda_local.py deploy --function all

# Invoke via LocalStack Lambda
python scripts/lambda_local.py invoke-remote baseline --event tests/events/baseline_event.json
```

This simulates the actual AWS Lambda execution environment.

