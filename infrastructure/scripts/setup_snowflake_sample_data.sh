#!/bin/bash
# =============================================================================
# CHALK AND DUSTER - Setup Sample Data in LocalStack Snowflake Emulator
# =============================================================================
# Run this script to create sample databases, tables, and data in the
# LocalStack Snowflake emulator for testing quality and drift checks.
#
# Usage:
#   ./infrastructure/scripts/setup_snowflake_sample_data.sh
#
# Or from Docker:
#   docker compose exec localstack /bin/bash -c "$(cat infrastructure/scripts/setup_snowflake_sample_data.sh)"

set -e

echo "=== Setting up Sample Data in LocalStack Snowflake Emulator ==="
echo ""

# Check if sflocal is available
if ! command -v sflocal &> /dev/null; then
    echo "ERROR: sflocal command not found."
    echo "This script must be run inside the LocalStack container."
    echo ""
    echo "Run with: docker compose exec localstack /bin/bash -c './infrastructure/scripts/setup_snowflake_sample_data.sh'"
    exit 1
fi

echo "Creating database TEST_DB..."
sflocal query "CREATE DATABASE IF NOT EXISTS TEST_DB" || echo "Database may already exist"

echo "Creating schema PUBLIC..."
sflocal query "CREATE SCHEMA IF NOT EXISTS TEST_DB.PUBLIC" || echo "Schema may already exist"

echo ""
echo "=== Creating Tables ==="

echo "Creating customers table..."
sflocal query "CREATE TABLE IF NOT EXISTS TEST_DB.PUBLIC.customers (
    customer_id INTEGER,
    name VARCHAR(255),
    email VARCHAR(255),
    signup_date DATE,
    is_active BOOLEAN,
    created_at TIMESTAMP
)"

echo "Creating products table..."
sflocal query "CREATE TABLE IF NOT EXISTS TEST_DB.PUBLIC.products (
    product_id INTEGER,
    name VARCHAR(255),
    price DECIMAL(10, 2),
    category VARCHAR(100),
    stock_quantity INTEGER,
    is_active BOOLEAN,
    created_at TIMESTAMP
)"

echo "Creating orders table..."
sflocal query "CREATE TABLE IF NOT EXISTS TEST_DB.PUBLIC.orders (
    order_id INTEGER,
    customer_id INTEGER,
    order_date TIMESTAMP,
    amount DECIMAL(10, 2),
    status VARCHAR(50),
    shipping_address VARCHAR(500),
    created_at TIMESTAMP
)"

echo ""
echo "=== Inserting Sample Data ==="

echo "Inserting customers..."
sflocal query "INSERT INTO TEST_DB.PUBLIC.customers (customer_id, name, email, signup_date, is_active) VALUES
    (1, 'John Doe', 'john@example.com', '2024-01-01', true),
    (2, 'Jane Smith', 'jane@example.com', '2024-01-05', true),
    (3, 'Bob Wilson', 'bob@example.com', '2024-01-10', true),
    (4, 'Alice Brown', 'alice@example.com', '2024-01-15', false),
    (5, 'Charlie Davis', 'charlie@example.com', '2024-01-20', true)"

echo "Inserting products..."
sflocal query "INSERT INTO TEST_DB.PUBLIC.products (product_id, name, price, category, stock_quantity, is_active) VALUES
    (1, 'Widget A', 19.99, 'Widgets', 100, true),
    (2, 'Widget B', 29.99, 'Widgets', 50, true),
    (3, 'Gadget X', 49.99, 'Gadgets', 75, true),
    (4, 'Gadget Y', 59.99, 'Gadgets', 30, true),
    (5, 'Tool Z', 39.99, 'Tools', 200, true),
    (6, 'Tool W', 24.99, 'Tools', 150, false)"

echo "Inserting orders..."
sflocal query "INSERT INTO TEST_DB.PUBLIC.orders (order_id, customer_id, order_date, amount, status) VALUES
    (1, 1, '2024-01-15 10:30:00', 49.98, 'completed'),
    (2, 2, '2024-01-16 14:20:00', 29.99, 'completed'),
    (3, 1, '2024-01-17 09:15:00', 99.97, 'pending'),
    (4, 3, '2024-01-18 16:45:00', 19.99, 'completed'),
    (5, 2, '2024-01-19 11:00:00', 79.98, 'cancelled'),
    (6, 4, '2024-01-20 09:00:00', 59.99, 'completed'),
    (7, 5, '2024-01-21 15:30:00', 149.97, 'pending'),
    (8, 1, '2024-01-22 12:00:00', 39.99, 'completed')"

echo ""
echo "=== Verifying Data ==="

echo "Customers count:"
sflocal query "SELECT COUNT(*) as count FROM TEST_DB.PUBLIC.customers"

echo "Products count:"
sflocal query "SELECT COUNT(*) as count FROM TEST_DB.PUBLIC.products"

echo "Orders count:"
sflocal query "SELECT COUNT(*) as count FROM TEST_DB.PUBLIC.orders"

echo ""
echo "=== Sample Data Setup Complete ==="
echo ""
echo "You can now run quality and drift checks against these tables:"
echo "  - TEST_DB.PUBLIC.customers"
echo "  - TEST_DB.PUBLIC.products"
echo "  - TEST_DB.PUBLIC.orders"
echo ""
echo "Connection details for Streamlit app:"
echo "  Account: test"
echo "  Database: TEST_DB"
echo "  Schema: PUBLIC"
echo "  (Credentials from secret_arn in Secrets Manager)"

