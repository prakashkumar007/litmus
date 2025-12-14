#!/bin/bash
# =============================================================================
# CHALK AND DUSTER - LocalStack Initialization
# =============================================================================
# This script runs when LocalStack is ready
# Includes Snowflake emulator setup for local development

echo "Initializing LocalStack resources for Chalk and Duster..."

# =============================================================================
# S3 BUCKETS (for Evidently baselines and reports)
# =============================================================================
echo "Creating S3 buckets..."
awslocal s3 mb s3://chalkandduster-baselines
awslocal s3 mb s3://chalkandduster-reports

# Create SQS Queues
echo "Creating SQS queues..."
awslocal sqs create-queue --queue-name chalkandduster-quality-checks
awslocal sqs create-queue --queue-name chalkandduster-drift-checks
awslocal sqs create-queue --queue-name chalkandduster-alerts
awslocal sqs create-queue --queue-name chalkandduster-quality-checks-dlq
awslocal sqs create-queue --queue-name chalkandduster-drift-checks-dlq

# Create SNS Topics
echo "Creating SNS topics..."
awslocal sns create-topic --name chalkandduster-events
awslocal sns create-topic --name chalkandduster-alerts

# Create sample secrets in Secrets Manager
echo "Creating sample secrets..."
awslocal secretsmanager create-secret \
    --name "chalkandduster/connections/sample-tenant/snowflake" \
    --secret-string '{
        "account": "test",
        "user": "test",
        "password": "test",
        "warehouse": "test_warehouse",
        "database": "test_db",
        "schema": "public",
        "role": "test_role"
    }'

# Subscribe alert queue to alert topic
ALERT_QUEUE_ARN=$(awslocal sqs get-queue-attributes \
    --queue-url http://localhost:4566/000000000000/chalkandduster-alerts \
    --attribute-names QueueArn \
    --query 'Attributes.QueueArn' \
    --output text)

ALERT_TOPIC_ARN=$(awslocal sns list-topics --query "Topics[?contains(TopicArn, 'chalkandduster-alerts')].TopicArn" --output text)

awslocal sns subscribe \
    --topic-arn "$ALERT_TOPIC_ARN" \
    --protocol sqs \
    --notification-endpoint "$ALERT_QUEUE_ARN"

# =============================================================================
# SNOWFLAKE EMULATOR SETUP
# =============================================================================
echo ""
echo "Setting up Snowflake emulator data..."

# Initialize Snowflake test database and tables using sflocal (LocalStack's Snowflake CLI)
# Note: sflocal commands will work once the Snowflake emulator is available

# Create a sample SQL file for Snowflake initialization
cat > /tmp/snowflake_init.sql << 'EOF'
-- Create test database and schema
CREATE DATABASE IF NOT EXISTS test_db;
USE DATABASE test_db;
CREATE SCHEMA IF NOT EXISTS public;
USE SCHEMA public;

-- Create sample warehouse
CREATE WAREHOUSE IF NOT EXISTS test_warehouse
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

-- Create sample tables for testing data quality
CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date TIMESTAMP_NTZ NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    category VARCHAR(100),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert sample data
INSERT INTO customers (customer_id, name, email) VALUES
    (1, 'John Doe', 'john@example.com'),
    (2, 'Jane Smith', 'jane@example.com'),
    (3, 'Bob Wilson', 'bob@example.com');

INSERT INTO products (product_id, name, price, category) VALUES
    (1, 'Widget A', 19.99, 'Widgets'),
    (2, 'Widget B', 29.99, 'Widgets'),
    (3, 'Gadget X', 49.99, 'Gadgets');

INSERT INTO orders (order_id, customer_id, order_date, amount, status) VALUES
    (1, 1, '2024-01-15 10:30:00', 49.98, 'completed'),
    (2, 2, '2024-01-16 14:20:00', 29.99, 'completed'),
    (3, 1, '2024-01-17 09:15:00', 99.97, 'pending'),
    (4, 3, '2024-01-18 16:45:00', 19.99, 'completed'),
    (5, 2, '2024-01-19 11:00:00', 79.98, 'cancelled');
EOF

echo "Snowflake initialization SQL created at /tmp/snowflake_init.sql"

# Execute SQL against Snowflake emulator using sflocal CLI
# sflocal is LocalStack's Snowflake CLI tool for interacting with the emulator
echo "Executing Snowflake initialization SQL..."

# Wait for Snowflake emulator to be ready (retry logic)
MAX_RETRIES=30
RETRY_COUNT=0
SNOWFLAKE_READY=false

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    # Try to execute a simple query to check if Snowflake is ready
    if sflocal query "SELECT 1" > /dev/null 2>&1; then
        SNOWFLAKE_READY=true
        echo "Snowflake emulator is ready!"
        break
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    echo "Waiting for Snowflake emulator... (attempt $RETRY_COUNT/$MAX_RETRIES)"
    sleep 2
done

if [ "$SNOWFLAKE_READY" = true ]; then
    echo "Creating Snowflake database and tables..."

    # Execute each statement separately (sflocal may not support multi-statement)
    sflocal query "CREATE DATABASE IF NOT EXISTS TEST_DB" || echo "Database may already exist"
    sflocal query "CREATE SCHEMA IF NOT EXISTS TEST_DB.PUBLIC" || echo "Schema may already exist"

    # Create customers table
    sflocal query "CREATE TABLE IF NOT EXISTS TEST_DB.PUBLIC.customers (
        customer_id INTEGER,
        name VARCHAR(255),
        email VARCHAR(255),
        created_at TIMESTAMP
    )" || echo "customers table may already exist"

    # Create products table
    sflocal query "CREATE TABLE IF NOT EXISTS TEST_DB.PUBLIC.products (
        product_id INTEGER,
        name VARCHAR(255),
        price DECIMAL(10, 2),
        category VARCHAR(100),
        created_at TIMESTAMP
    )" || echo "products table may already exist"

    # Create orders table
    sflocal query "CREATE TABLE IF NOT EXISTS TEST_DB.PUBLIC.orders (
        order_id INTEGER,
        customer_id INTEGER,
        order_date TIMESTAMP,
        amount DECIMAL(10, 2),
        status VARCHAR(50),
        created_at TIMESTAMP
    )" || echo "orders table may already exist"

    # Insert sample data into customers
    sflocal query "INSERT INTO TEST_DB.PUBLIC.customers (customer_id, name, email) VALUES
        (1, 'John Doe', 'john@example.com'),
        (2, 'Jane Smith', 'jane@example.com'),
        (3, 'Bob Wilson', 'bob@example.com')" || echo "Customer data may already exist"

    # Insert sample data into products
    sflocal query "INSERT INTO TEST_DB.PUBLIC.products (product_id, name, price, category) VALUES
        (1, 'Widget A', 19.99, 'Widgets'),
        (2, 'Widget B', 29.99, 'Widgets'),
        (3, 'Gadget X', 49.99, 'Gadgets'),
        (4, 'Gadget Y', 59.99, 'Gadgets'),
        (5, 'Tool Z', 39.99, 'Tools')" || echo "Product data may already exist"

    # Insert sample data into orders
    sflocal query "INSERT INTO TEST_DB.PUBLIC.orders (order_id, customer_id, order_date, amount, status) VALUES
        (1, 1, '2024-01-15 10:30:00', 49.98, 'completed'),
        (2, 2, '2024-01-16 14:20:00', 29.99, 'completed'),
        (3, 1, '2024-01-17 09:15:00', 99.97, 'pending'),
        (4, 3, '2024-01-18 16:45:00', 19.99, 'completed'),
        (5, 2, '2024-01-19 11:00:00', 79.98, 'cancelled')" || echo "Order data may already exist"

    echo "Snowflake sample data setup complete!"

    # Verify the data was created
    echo ""
    echo "=== Verifying Snowflake Data ==="
    sflocal query "SELECT COUNT(*) as count FROM TEST_DB.PUBLIC.customers" || true
    sflocal query "SELECT COUNT(*) as count FROM TEST_DB.PUBLIC.products" || true
    sflocal query "SELECT COUNT(*) as count FROM TEST_DB.PUBLIC.orders" || true
else
    echo "WARNING: Snowflake emulator not ready after $MAX_RETRIES attempts"
    echo "You can manually run the SQL from /tmp/snowflake_init.sql later"
fi

echo ""
echo "LocalStack initialization complete!"

# List created resources
echo ""
echo "=== Created S3 Buckets ==="
awslocal s3 ls

echo ""
echo "=== Created SQS Queues ==="
awslocal sqs list-queues

echo ""
echo "=== Created SNS Topics ==="
awslocal sns list-topics

echo ""
echo "=== Created Secrets ==="
awslocal secretsmanager list-secrets --query 'SecretList[].Name'

echo ""
echo "=== Snowflake Emulator ==="
echo "Snowflake emulator endpoint: snowflake.localhost.localstack.cloud:4566"
echo "Test credentials: user=test, password=test, account=test"

echo ""
echo "=== Lambda Testing ==="
echo "Use the scripts/lambda_local.py script to test Lambda functions locally:"
echo "  python scripts/lambda_local.py invoke baseline --event tests/events/baseline_event.json"
echo "  python scripts/lambda_local.py invoke drift --event tests/events/drift_event.json"
echo "  python scripts/lambda_local.py invoke quality --event tests/events/quality_event.json"

