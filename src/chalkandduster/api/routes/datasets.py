"""
Chalk and Duster - Dataset Routes
"""

from datetime import datetime
from uuid import UUID

import structlog
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from chalkandduster.api.deps import get_db_session
from chalkandduster.api.schemas.dataset import (
    DatasetCreate,
    DatasetResponse,
    DatasetUpdate,
    DatasetValidation,
    DatasetTriggerRequest,
    DatasetTriggerResponse,
)
from chalkandduster.db.postgres import crud
from chalkandduster.db.postgres.session import async_session_factory
from chalkandduster.quality.validator import validate_quality_yaml, validate_drift_yaml
from chalkandduster.observability.metrics import (
    record_quality_check,
    record_drift_detection,
    QUALITY_CHECKS_FAILED,
    DRIFT_DETECTED,
)

logger = structlog.get_logger()
router = APIRouter()


@router.post("", response_model=DatasetResponse, status_code=status.HTTP_201_CREATED)
async def create_dataset(
    dataset_in: DatasetCreate,
    db: AsyncSession = Depends(get_db_session),
) -> DatasetResponse:
    """
    Create a new dataset for monitoring.
    """
    # Verify tenant exists
    tenant = await crud.get_tenant_by_id(db, dataset_in.tenant_id)
    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Tenant not found",
        )
    
    # Verify connection exists
    connection = await crud.get_connection_by_id(db, dataset_in.connection_id)
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connection not found",
        )
    
    # Validate YAML if provided
    if dataset_in.quality_yaml:
        validation = validate_quality_yaml(dataset_in.quality_yaml)
        if not validation.valid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "Invalid quality YAML", "errors": validation.errors},
            )
    
    if dataset_in.drift_yaml:
        validation = validate_drift_yaml(dataset_in.drift_yaml)
        if not validation.valid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "Invalid drift YAML", "errors": validation.errors},
            )
    
    # Create dataset
    dataset = await crud.create_dataset(db, dataset_in)
    logger.info(
        "Dataset created",
        dataset_id=str(dataset.id),
        tenant_id=str(dataset.tenant_id),
    )
    
    return DatasetResponse.model_validate(dataset)


@router.get("", response_model=list[DatasetResponse])
async def list_datasets(
    tenant_id: UUID = Query(...),
    connection_id: UUID = Query(None),
    is_active: bool = Query(None),
    tags: list[str] = Query(None),
    db: AsyncSession = Depends(get_db_session),
) -> list[DatasetResponse]:
    """
    List datasets for a tenant.
    """
    datasets = await crud.list_datasets(
        db,
        tenant_id=tenant_id,
        connection_id=connection_id,
        is_active=is_active,
        tags=tags,
    )
    return [DatasetResponse.model_validate(d) for d in datasets]


@router.get("/{dataset_id}", response_model=DatasetResponse)
async def get_dataset(
    dataset_id: UUID,
    db: AsyncSession = Depends(get_db_session),
) -> DatasetResponse:
    """
    Get a dataset by ID.
    """
    dataset = await crud.get_dataset_by_id(db, dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found",
        )
    
    return DatasetResponse.model_validate(dataset)


@router.patch("/{dataset_id}", response_model=DatasetResponse)
async def update_dataset(
    dataset_id: UUID,
    dataset_in: DatasetUpdate,
    db: AsyncSession = Depends(get_db_session),
) -> DatasetResponse:
    """
    Update a dataset.
    """
    dataset = await crud.get_dataset_by_id(db, dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found",
        )
    
    # Validate YAML if provided
    if dataset_in.quality_yaml:
        validation = validate_quality_yaml(dataset_in.quality_yaml)
        if not validation.valid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "Invalid quality YAML", "errors": validation.errors},
            )
    
    if dataset_in.drift_yaml:
        validation = validate_drift_yaml(dataset_in.drift_yaml)
        if not validation.valid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "Invalid drift YAML", "errors": validation.errors},
            )
    
    dataset = await crud.update_dataset(db, dataset, dataset_in)
    logger.info("Dataset updated", dataset_id=str(dataset.id))
    
    return DatasetResponse.model_validate(dataset)


@router.post("/{dataset_id}/validate", response_model=DatasetValidation)
async def validate_dataset_yaml(
    dataset_id: UUID,
    db: AsyncSession = Depends(get_db_session),
) -> DatasetValidation:
    """
    Validate the YAML configuration for a dataset.
    """
    dataset = await crud.get_dataset_by_id(db, dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found",
        )
    
    errors = []
    warnings = []
    check_count = 0
    monitor_count = 0
    
    if dataset.quality_yaml:
        validation = validate_quality_yaml(dataset.quality_yaml)
        errors.extend(validation.errors)
        warnings.extend(validation.warnings)
        check_count = validation.check_count
    
    if dataset.drift_yaml:
        validation = validate_drift_yaml(dataset.drift_yaml)
        errors.extend(validation.errors)
        warnings.extend(validation.warnings)
        monitor_count = validation.monitor_count
    
    return DatasetValidation(
        valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        check_count=check_count,
        monitor_count=monitor_count,
    )


async def _run_quality_check(run_id: UUID, dataset_id: UUID, tenant_id: UUID):
    """Background task to run quality checks using Soda Core."""
    from chalkandduster.quality.executor import QualityExecutor

    async with async_session_factory() as db:
        run = await crud.get_run_by_id(db, run_id)
        if not run:
            return

        dataset = await crud.get_dataset_by_id(db, dataset_id)
        if not dataset:
            return

        # Get connection for credentials
        connection = await crud.get_connection_by_id(db, dataset.connection_id)
        if not connection:
            logger.error("Connection not found", connection_id=str(dataset.connection_id))
            return

        started_at = datetime.utcnow()

        # Update run to running status
        await crud.update_run(db, run, status="running", started_at=started_at)

        try:
            # Get credentials from Secrets Manager
            from chalkandduster.connectors.secrets import get_connection_credentials
            credentials = await get_connection_credentials(connection.id)

            # Build connection config for Soda Core
            connection_config = {
                "account": connection.account,
                "user": credentials.get("user", ""),
                "password": credentials.get("password"),
                "private_key": credentials.get("private_key"),
                "private_key_passphrase": credentials.get("private_key_passphrase"),
                "database": connection.database_name,
                "schema": connection.schema_name,
                "warehouse": connection.warehouse,
                "role": connection.role_name,
            }

            # Create executor and run quality checks
            executor = QualityExecutor(connection_config=connection_config)
            result = await executor.execute(
                dataset_id=dataset_id,
                quality_yaml=dataset.quality_yaml or "checks: {}",
                table_name=dataset.table_name,
            )

            completed_at = datetime.utcnow()
            duration = (completed_at - started_at).total_seconds()

            # Convert results to dict format
            results_list = [r.to_dict() for r in result.results]

            # Record failed check metrics
            for check_result in result.results:
                if check_result.status == "failed":
                    QUALITY_CHECKS_FAILED.labels(
                        tenant_id=str(tenant_id),
                        dataset_id=str(dataset_id),
                        check_name=check_result.check_name[:50],
                        severity=check_result.severity,
                    ).inc()

            # Update run with results
            await crud.update_run(
                db, run,
                status="completed",
                completed_at=completed_at,
                duration_seconds=duration,
                total_checks=result.total_checks,
                passed_checks=result.passed_checks,
                failed_checks=result.failed_checks,
                error_checks=result.error_checks,
                results={"checks": results_list},
            )

            # Record metrics
            check_status = "success" if result.failed_checks == 0 else "failure"
            record_quality_check(
                tenant_id=str(tenant_id),
                dataset_id=str(dataset_id),
                status=check_status,
                duration_seconds=duration,
                failed_checks=result.failed_checks,
            )

            # Update dataset last run info
            dataset.last_quality_run_at = completed_at
            dataset.last_quality_status = check_status
            await db.commit()

            logger.info(
                "Quality check completed",
                run_id=str(run_id),
                total=result.total_checks,
                passed=result.passed_checks,
                failed=result.failed_checks,
            )

        except Exception as e:
            logger.error("Quality check failed", run_id=str(run_id), error=str(e))
            await crud.update_run(
                db, run,
                status="failed",
                completed_at=datetime.utcnow(),
                error_message=str(e),
            )
            record_quality_check(
                tenant_id=str(tenant_id),
                dataset_id=str(dataset_id),
                status="error",
                duration_seconds=0,
            )


async def _run_drift_detection(run_id: UUID, dataset_id: UUID, tenant_id: UUID):
    """Background task to run drift detection using DriftDetector."""
    from chalkandduster.drift.detector import DriftDetector
    from chalkandduster.connectors.snowflake import SnowflakeConnector

    async with async_session_factory() as db:
        run = await crud.get_run_by_id(db, run_id)
        if not run:
            return

        dataset = await crud.get_dataset_by_id(db, dataset_id)
        if not dataset:
            return

        # Get connection for credentials
        connection = await crud.get_connection_by_id(db, dataset.connection_id)
        if not connection:
            logger.error("Connection not found", connection_id=str(dataset.connection_id))
            return

        started_at = datetime.utcnow()

        # Update run to running status
        await crud.update_run(db, run, status="running", started_at=started_at)

        try:
            # Get credentials from Secrets Manager
            from chalkandduster.connectors.secrets import get_connection_credentials
            credentials = await get_connection_credentials(connection.id)

            # Create Snowflake connector
            connector = SnowflakeConnector(
                account=connection.account,
                user=credentials.get("user", ""),
                password=credentials.get("password"),
                private_key=credentials.get("private_key"),
                private_key_passphrase=credentials.get("private_key_passphrase"),
                warehouse=connection.warehouse,
                database=connection.database_name,
                schema=connection.schema_name,
                role=connection.role_name,
            )

            # Create detector and run drift detection
            detector = DriftDetector(snowflake_connector=connector)
            result = await detector.detect(
                dataset_id=dataset_id,
                drift_yaml=dataset.drift_yaml or "monitors: []",
                table_name=dataset.table_name,
                database=connection.database_name,
                schema=connection.schema_name,
            )

            completed_at = datetime.utcnow()
            duration = (completed_at - started_at).total_seconds()

            # Convert results to dict format
            results_list = [r.to_dict() for r in result.results]
            drift_count = result.drift_detected_count

            # Record drift detected metrics
            for drift_result in result.results:
                if drift_result.detected:
                    DRIFT_DETECTED.labels(
                        tenant_id=str(tenant_id),
                        dataset_id=str(dataset_id),
                        drift_type=drift_result.drift_type,
                    ).inc()

            # Update run with results
            await crud.update_run(
                db, run,
                status="completed",
                completed_at=completed_at,
                duration_seconds=duration,
                total_checks=result.total_monitors,
                passed_checks=result.total_monitors - drift_count,
                failed_checks=drift_count,
                results={"monitors": results_list},
            )

            # Record metrics
            check_status = "success" if drift_count == 0 else "drift_detected"
            record_drift_detection(
                tenant_id=str(tenant_id),
                dataset_id=str(dataset_id),
                status=check_status,
                duration_seconds=duration,
                drift_count=drift_count,
            )

            # Update dataset last run info
            dataset.last_drift_run_at = completed_at
            dataset.last_drift_status = check_status
            await db.commit()

            logger.info(
                "Drift detection completed",
                run_id=str(run_id),
                total=result.total_monitors,
                drifts_detected=drift_count,
            )

            # Clean up connection
            connector.close()

        except Exception as e:
            logger.error("Drift detection failed", run_id=str(run_id), error=str(e))
            await crud.update_run(
                db, run,
                status="failed",
                completed_at=datetime.utcnow(),
                error_message=str(e),
            )
            record_drift_detection(
                tenant_id=str(tenant_id),
                dataset_id=str(dataset_id),
                status="error",
                duration_seconds=0,
            )


@router.post("/{dataset_id}/trigger", response_model=DatasetTriggerResponse)
async def trigger_dataset_check(
    dataset_id: UUID,
    request: DatasetTriggerRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db_session),
) -> DatasetTriggerResponse:
    """
    Manually trigger a quality check or drift detection for a dataset.

    - **check_type**: 'quality', 'drift', or 'both'
    - **force**: If true, run even if a check is already in progress
    """
    dataset = await crud.get_dataset_by_id(db, dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found",
        )

    run_ids = []

    if request.check_type in ("quality", "both"):
        if not dataset.quality_yaml:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Dataset has no quality YAML configured",
            )

        # Create quality run
        quality_run = await crud.create_run(
            db,
            dataset_id=dataset_id,
            tenant_id=dataset.tenant_id,
            run_type="quality",
        )
        run_ids.append(str(quality_run.id))

        # Schedule background task
        background_tasks.add_task(
            _run_quality_check,
            quality_run.id,
            dataset_id,
            dataset.tenant_id,
        )

        logger.info("Quality check triggered", dataset_id=str(dataset_id), run_id=str(quality_run.id))

    if request.check_type in ("drift", "both"):
        if not dataset.drift_yaml:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Dataset has no drift YAML configured",
            )

        # Create drift run
        drift_run = await crud.create_run(
            db,
            dataset_id=dataset_id,
            tenant_id=dataset.tenant_id,
            run_type="drift",
        )
        run_ids.append(str(drift_run.id))

        # Schedule background task
        background_tasks.add_task(
            _run_drift_detection,
            drift_run.id,
            dataset_id,
            dataset.tenant_id,
        )

        logger.info("Drift detection triggered", dataset_id=str(dataset_id), run_id=str(drift_run.id))

    return DatasetTriggerResponse(
        success=True,
        run_id=UUID(run_ids[0]),  # Return first run ID
        check_type=request.check_type,
        message=f"Triggered {request.check_type} check(s). Run ID(s): {', '.join(run_ids)}",
    )
