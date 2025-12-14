"""
Chalk and Duster - Drift Detection DAG

This DAG runs drift detection for all active datasets.
It can be triggered on a schedule or manually.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    "owner": "chalkandduster",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Create the DAG
with DAG(
    dag_id="chalkandduster_drift_detection",
    default_args=default_args,
    description="Run drift detection for all active datasets",
    schedule_interval="0 6 * * *",  # Daily at 6 AM
    start_date=days_ago(1),
    catchup=False,
    tags=["chalkandduster", "drift"],
) as dag:
    
    @task
    def get_active_datasets() -> list:
        """Fetch all active datasets that need drift detection."""
        import asyncio
        from sqlalchemy import select
        from chalkandduster.db.postgres.session import async_session_factory
        from chalkandduster.db.postgres.models import Dataset
        
        async def fetch_datasets():
            async with async_session_factory() as session:
                result = await session.execute(
                    select(Dataset).where(
                        Dataset.is_active == True,
                        Dataset.drift_yaml.isnot(None),
                    )
                )
                datasets = result.scalars().all()
                return [
                    {
                        "id": str(d.id),
                        "name": d.name,
                        "tenant_id": str(d.tenant_id),
                        "connection_id": str(d.connection_id),
                        "table_name": d.table_name,
                        "database_name": d.database_name,
                        "schema_name": d.schema_name,
                        "drift_yaml": d.drift_yaml,
                    }
                    for d in datasets
                ]
        
        return asyncio.run(fetch_datasets())
    
    @task
    def run_drift_detection(dataset: Dict[str, Any]) -> Dict[str, Any]:
        """Run drift detection for a single dataset."""
        import asyncio
        from datetime import datetime
        from uuid import UUID
        from chalkandduster.drift import get_drift_detector
        from chalkandduster.connectors.snowflake import SnowflakeConnector
        from chalkandduster.connectors.secrets import get_connection_credentials
        from chalkandduster.db.postgres.session import async_session_factory
        from chalkandduster.db.postgres import crud

        async def execute_detection():
            dataset_id = UUID(dataset["id"])
            tenant_id = UUID(dataset["tenant_id"])
            connection_id = UUID(dataset["connection_id"])

            async with async_session_factory() as db:
                # Create run record with scheduled trigger type
                run = await crud.create_run(
                    db,
                    dataset_id=dataset_id,
                    tenant_id=tenant_id,
                    run_type="drift",
                    trigger_type="scheduled",
                )

                start_time = datetime.now(timezone.utc)

                try:
                    # Get connection details
                    connection = await crud.get_connection_by_id(db, connection_id)
                    if not connection:
                        raise ValueError(f"Connection {connection_id} not found")

                    # Get credentials from secrets manager
                    credentials = await get_connection_credentials(connection.secret_arn)

                    # Update run to running
                    await crud.update_run(db, run, status="running", started_at=start_time)

                    # Create Snowflake connector
                    connector = SnowflakeConnector(
                        account=connection.account,
                        user=credentials.get("user"),
                        password=credentials.get("password"),
                        private_key=credentials.get("private_key"),
                        warehouse=connection.warehouse,
                        database=connection.database_name,
                        schema=connection.schema_name,
                        role=connection.role_name,
                    )

                    detector = get_drift_detector(snowflake_connector=connector)

                    result = await detector.detect(
                        dataset_id=dataset_id,
                        drift_yaml=dataset["drift_yaml"],
                        table_name=dataset["table_name"],
                        database=dataset["database_name"],
                        schema=dataset["schema_name"],
                    )

                    end_time = datetime.now(timezone.utc)
                    duration = (end_time - start_time).total_seconds()

                    # Count drift detections as failures
                    drift_results = result.to_dict().get("results", [])
                    drifts_detected = sum(1 for r in drift_results if r.get("detected", False))

                    # Update run with results
                    await crud.update_run(
                        db, run,
                        status="completed",
                        completed_at=end_time,
                        duration_seconds=duration,
                        total_checks=len(drift_results),
                        passed_checks=len(drift_results) - drifts_detected,
                        failed_checks=drifts_detected,
                        results=drift_results,
                    )

                    return result.to_dict()

                except Exception as e:
                    end_time = datetime.now(timezone.utc)
                    duration = (end_time - start_time).total_seconds()

                    await crud.update_run(
                        db, run,
                        status="failed",
                        completed_at=end_time,
                        duration_seconds=duration,
                        error_message=str(e),
                    )
                    raise

        return asyncio.run(execute_detection())
    
    @task
    def process_results(results: list) -> Dict[str, Any]:
        """Process drift detection results and send alerts if needed."""
        import asyncio
        from chalkandduster.alerting.slack import SlackNotifier
        from chalkandduster.llm.drift_explainer import explain_drift
        
        async def process():
            notifier = SlackNotifier()
            alerts_sent = 0
            
            for result in results:
                drift_detected = [
                    r for r in result.get("results", [])
                    if r.get("detected")
                ]
                
                if drift_detected:
                    # Explain drift with LLM
                    explanation = await explain_drift(
                        dataset_name=result.get("dataset_id", "Unknown"),
                        drift_results=drift_detected,
                    )
                    
                    # Send Slack alert
                    await notifier.send_drift_alert(
                        dataset_name=result.get("dataset_id", "Unknown"),
                        run_id=result.get("run_id", "Unknown"),
                        drift_results=drift_detected,
                        summary=explanation.summary,
                    )
                    alerts_sent += 1
            
            return {
                "total_datasets": len(results),
                "alerts_sent": alerts_sent,
            }
        
        return asyncio.run(process())
    
    # Define task dependencies
    datasets = get_active_datasets()
    results = run_drift_detection.expand(dataset=datasets)
    summary = process_results(results)

