-- =============================================================================
-- CHALK AND DUSTER - PostgreSQL Initialization
-- =============================================================================
-- This script runs when the PostgreSQL container first starts

-- Create additional databases for Airflow
CREATE DATABASE airflow;
CREATE USER airflow WITH ENCRYPTED PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

-- Create extensions
\c chalkandduster;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- =============================================================================
-- TENANTS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS tenants (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL UNIQUE,
    slug VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    
    -- Slack configuration
    slack_webhook_url TEXT,
    slack_channel VARCHAR(100),
    
    -- Snowflake configuration reference
    snowflake_account VARCHAR(255),
    snowflake_database VARCHAR(255),
    
    -- Settings
    settings JSONB DEFAULT '{}',
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    
    -- Audit
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by VARCHAR(255),
    
    -- Constraints
    CONSTRAINT slug_format CHECK (slug ~ '^[a-z0-9-]+$')
);

-- =============================================================================
-- CONNECTIONS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS connections (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    
    name VARCHAR(255) NOT NULL,
    connection_type VARCHAR(50) NOT NULL DEFAULT 'snowflake',
    
    -- Connection details (sensitive data in Secrets Manager)
    account VARCHAR(255),
    warehouse VARCHAR(255),
    database_name VARCHAR(255),
    schema_name VARCHAR(255),
    role_name VARCHAR(255),
    
    -- Reference to AWS Secrets Manager
    secret_arn VARCHAR(500),
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    last_tested_at TIMESTAMP WITH TIME ZONE,
    last_test_status VARCHAR(50),
    
    -- Audit
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT unique_tenant_connection UNIQUE (tenant_id, name)
);

-- =============================================================================
-- DATASETS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS datasets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    connection_id UUID NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
    
    name VARCHAR(255) NOT NULL,
    description TEXT,
    
    -- Table reference
    database_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    
    -- YAML configurations (stored as text for easy editing)
    quality_yaml TEXT,
    drift_yaml TEXT,
    
    -- Scheduling
    quality_schedule VARCHAR(100),  -- Cron expression
    drift_schedule VARCHAR(100),    -- Cron expression
    
    -- Tags
    tags JSONB DEFAULT '[]',
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    
    -- Last run info
    last_quality_run_at TIMESTAMP WITH TIME ZONE,
    last_quality_status VARCHAR(50),
    last_drift_run_at TIMESTAMP WITH TIME ZONE,
    last_drift_status VARCHAR(50),
    
    -- Audit
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT unique_tenant_dataset UNIQUE (tenant_id, name)
);

-- =============================================================================
-- RUNS TABLE (Quality Check & Drift Detection Runs)
-- =============================================================================
CREATE TABLE IF NOT EXISTS runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,

    run_type VARCHAR(50) NOT NULL,  -- 'quality' or 'drift'
    trigger_type VARCHAR(50) NOT NULL DEFAULT 'on_demand',  -- 'on_demand' or 'scheduled'
    status VARCHAR(50) NOT NULL DEFAULT 'pending',  -- pending, running, completed, failed

    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_seconds FLOAT,

    -- Results summary
    total_checks INTEGER DEFAULT 0,
    passed_checks INTEGER DEFAULT 0,
    failed_checks INTEGER DEFAULT 0,
    error_checks INTEGER DEFAULT 0,

    -- Detailed results
    results JSONB,
    error_message TEXT,

    -- Audit
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- =============================================================================
-- PROFILES TABLE (Data Profiling Results)
-- =============================================================================
CREATE TABLE IF NOT EXISTS profiles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    
    -- Profile data
    row_count BIGINT,
    column_count INTEGER,
    profile_data JSONB NOT NULL,  -- Detailed column statistics
    
    -- Timing
    profiled_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    duration_seconds FLOAT,
    
    CONSTRAINT unique_latest_profile UNIQUE (dataset_id, profiled_at)
);

-- =============================================================================
-- AUDIT LOG TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS audit_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID REFERENCES tenants(id),
    
    -- Event details
    event_type VARCHAR(100) NOT NULL,
    event_action VARCHAR(50) NOT NULL,
    resource_type VARCHAR(100),
    resource_id UUID,
    
    -- Actor
    actor_id VARCHAR(255),
    actor_type VARCHAR(50),  -- user, system, api
    
    -- Details
    details JSONB,
    ip_address INET,
    user_agent TEXT,
    
    -- Timing
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- =============================================================================
-- INDEXES
-- =============================================================================
CREATE INDEX idx_tenants_slug ON tenants(slug);
CREATE INDEX idx_connections_tenant ON connections(tenant_id);
CREATE INDEX idx_datasets_tenant ON datasets(tenant_id);
CREATE INDEX idx_datasets_connection ON datasets(connection_id);
CREATE INDEX idx_profiles_dataset ON profiles(dataset_id);
CREATE INDEX idx_runs_dataset ON runs(dataset_id);
CREATE INDEX idx_runs_tenant ON runs(tenant_id);
CREATE INDEX idx_runs_type ON runs(run_type);
CREATE INDEX idx_runs_trigger_type ON runs(trigger_type);
CREATE INDEX idx_runs_status ON runs(status);
CREATE INDEX idx_runs_created ON runs(created_at);
CREATE INDEX idx_audit_log_tenant ON audit_log(tenant_id);
CREATE INDEX idx_audit_log_created ON audit_log(created_at);
CREATE INDEX idx_audit_log_event ON audit_log(event_type, event_action);

-- =============================================================================
-- TRIGGERS
-- =============================================================================
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_tenants_updated
    BEFORE UPDATE ON tenants
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trigger_connections_updated
    BEFORE UPDATE ON connections
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trigger_datasets_updated
    BEFORE UPDATE ON datasets
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trigger_runs_updated
    BEFORE UPDATE ON runs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
