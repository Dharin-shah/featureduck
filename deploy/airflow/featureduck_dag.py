"""
FeatureDuck Airflow DAG Template

This DAG orchestrates feature materialization using the FeatureDuck CLI.
Customize feature views, schedules, and dependencies for your use case.

Setup:
1. Install featureduck CLI in your Airflow environment
2. Configure connection 'featureduck_storage' for S3/GCS access (if needed)
3. Set Airflow variables:
   - featureduck_registry_path: Path to registry database
   - featureduck_storage_path: Base path for feature storage
4. Copy this file to your Airflow DAGs folder

Usage:
  - Trigger manually from Airflow UI
  - Set schedule_interval for automatic runs
  - Use sensors for data availability checks
"""

from datetime import datetime, timedelta
from typing import List, Optional

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule


# =============================================================================
# Configuration
# =============================================================================

# Feature views to materialize
FEATURE_VIEWS = [
    {
        "name": "user_features",
        "schedule": "hourly",
        "ttl_hours": 1,
        "dependencies": [],
        "priority": 1,
    },
    {
        "name": "purchase_features",
        "schedule": "hourly",
        "ttl_hours": 1,
        "dependencies": ["user_features"],
        "priority": 2,
    },
    {
        "name": "session_features",
        "schedule": "daily",
        "ttl_hours": 24,
        "dependencies": [],
        "priority": 1,
    },
]

# Default arguments for all tasks
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# Get configuration from Airflow Variables
REGISTRY_PATH = Variable.get("featureduck_registry_path", default_var="/data/registry/registry.db")
STORAGE_PATH = Variable.get("featureduck_storage_path", default_var="/data/features")
CLI_PATH = Variable.get("featureduck_cli_path", default_var="featureduck")


# =============================================================================
# Helper Functions
# =============================================================================

def get_time_range(ttl_hours: int, **context) -> tuple:
    """Calculate start and end time based on execution date."""
    execution_date = context["execution_date"]
    end_time = execution_date
    start_time = execution_date - timedelta(hours=ttl_hours)
    return start_time.isoformat(), end_time.isoformat()


def check_source_data_available(feature_view: str, **context) -> bool:
    """Check if source data is available for materialization."""
    # Implement your data availability check here
    # Examples:
    # - Check S3 partition exists
    # - Query source database for new records
    # - Check Kafka topic offset
    return True


def should_materialize(feature_view: str, **context) -> str:
    """Decide whether to run materialization or skip."""
    if check_source_data_available(feature_view, **context):
        return f"materialize_{feature_view}"
    return f"skip_{feature_view}"


# =============================================================================
# DAG: Hourly Feature Materialization
# =============================================================================

with DAG(
    dag_id="featureduck_hourly_materialization",
    description="Hourly feature materialization pipeline",
    default_args=default_args,
    schedule_interval="0 * * * *",  # Every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["featureduck", "feature-store", "hourly"],
) as hourly_dag:

    # Filter hourly feature views
    hourly_views = [fv for fv in FEATURE_VIEWS if fv["schedule"] == "hourly"]

    # Group feature views by priority for parallel execution
    priority_groups = {}
    for fv in hourly_views:
        priority = fv["priority"]
        if priority not in priority_groups:
            priority_groups[priority] = []
        priority_groups[priority].append(fv)

    previous_tasks = []

    for priority in sorted(priority_groups.keys()):
        group_tasks = []

        for fv in priority_groups[priority]:
            view_name = fv["name"]
            ttl_hours = fv["ttl_hours"]

            # Materialize task
            materialize = BashOperator(
                task_id=f"materialize_{view_name}",
                bash_command=f"""
                    {CLI_PATH} materialize \\
                        --name={view_name} \\
                        --start=-{ttl_hours}h \\
                        --end=now \\
                        --registry={REGISTRY_PATH}
                """,
                env={
                    "FEATUREDUCK_STORAGE": STORAGE_PATH,
                    "RUST_LOG": "info",
                },
            )

            # Validate task
            validate = BashOperator(
                task_id=f"validate_{view_name}",
                bash_command=f"""
                    {CLI_PATH} validate \\
                        --name={view_name} \\
                        --check-storage \\
                        --registry={REGISTRY_PATH}
                """,
                trigger_rule=TriggerRule.ALL_SUCCESS,
            )

            materialize >> validate
            group_tasks.append(materialize)

        # Set dependencies from previous priority group
        for prev_task in previous_tasks:
            for curr_task in group_tasks:
                prev_task >> curr_task

        previous_tasks = group_tasks


# =============================================================================
# DAG: Daily Feature Materialization
# =============================================================================

with DAG(
    dag_id="featureduck_daily_materialization",
    description="Daily feature materialization pipeline",
    default_args=default_args,
    schedule_interval="0 2 * * *",  # 2 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["featureduck", "feature-store", "daily"],
) as daily_dag:

    daily_views = [fv for fv in FEATURE_VIEWS if fv["schedule"] == "daily"]

    for fv in daily_views:
        view_name = fv["name"]

        materialize = BashOperator(
            task_id=f"materialize_{view_name}",
            bash_command=f"""
                {CLI_PATH} materialize \\
                    --name={view_name} \\
                    --start=-24h \\
                    --end=now \\
                    --registry={REGISTRY_PATH}
            """,
            env={
                "FEATUREDUCK_STORAGE": STORAGE_PATH,
                "RUST_LOG": "info",
            },
        )

        validate = BashOperator(
            task_id=f"validate_{view_name}",
            bash_command=f"""
                {CLI_PATH} validate \\
                    --name={view_name} \\
                    --check-storage \\
                    --check-schema \\
                    --registry={REGISTRY_PATH}
            """,
        )

        materialize >> validate


# =============================================================================
# DAG: Weekly Backfill
# =============================================================================

with DAG(
    dag_id="featureduck_weekly_backfill",
    description="Weekly feature backfill for data corrections",
    default_args={
        **default_args,
        "retries": 1,
        "execution_timeout": timedelta(hours=6),
    },
    schedule_interval="0 3 * * 0",  # 3 AM on Sundays
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["featureduck", "feature-store", "backfill"],
) as backfill_dag:

    @task
    def calculate_date_range(**context):
        """Calculate backfill date range."""
        end_date = context["execution_date"].strftime("%Y-%m-%d")
        start_date = (context["execution_date"] - timedelta(days=7)).strftime("%Y-%m-%d")
        return {"start_date": start_date, "end_date": end_date}

    date_range = calculate_date_range()

    for fv in FEATURE_VIEWS:
        view_name = fv["name"]

        backfill = BashOperator(
            task_id=f"backfill_{view_name}",
            bash_command=f"""
                {CLI_PATH} backfill \\
                    --name={view_name} \\
                    --start-date={{{{ ti.xcom_pull(task_ids='calculate_date_range')['start_date'] }}}} \\
                    --end-date={{{{ ti.xcom_pull(task_ids='calculate_date_range')['end_date'] }}}} \\
                    --parallelism=8 \\
                    --registry={REGISTRY_PATH}
            """,
            env={
                "FEATUREDUCK_STORAGE": STORAGE_PATH,
                "RUST_LOG": "info",
            },
        )

        date_range >> backfill


# =============================================================================
# DAG: Ad-hoc Materialization (Manual Trigger)
# =============================================================================

with DAG(
    dag_id="featureduck_adhoc_materialization",
    description="Manual trigger for ad-hoc feature materialization",
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        "feature_view": "user_features",
        "start_time": "-24h",
        "end_time": "now",
        "force": False,
    },
    tags=["featureduck", "feature-store", "adhoc"],
) as adhoc_dag:

    materialize = BashOperator(
        task_id="materialize_adhoc",
        bash_command=f"""
            {CLI_PATH} materialize \\
                --name={{{{ params.feature_view }}}} \\
                --start={{{{ params.start_time }}}} \\
                --end={{{{ params.end_time }}}} \\
                {{% if params.force %}}--force{{% endif %}} \\
                --registry={REGISTRY_PATH}
        """,
        env={
            "FEATUREDUCK_STORAGE": STORAGE_PATH,
            "RUST_LOG": "info,featureduck=debug",
        },
    )

    validate = BashOperator(
        task_id="validate_adhoc",
        bash_command=f"""
            {CLI_PATH} validate \\
                --name={{{{ params.feature_view }}}} \\
                --check-storage \\
                --check-schema \\
                --registry={REGISTRY_PATH}
        """,
    )

    show_stats = BashOperator(
        task_id="show_stats",
        bash_command=f"""
            {CLI_PATH} show {{{{ params.feature_view }}}} \\
                --stats \\
                --output=json \\
                --registry={REGISTRY_PATH}
        """,
    )

    materialize >> validate >> show_stats


# =============================================================================
# DAG: Feature Store Health Check
# =============================================================================

with DAG(
    dag_id="featureduck_health_check",
    description="Feature store health and data quality checks",
    default_args=default_args,
    schedule_interval="*/15 * * * *",  # Every 15 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["featureduck", "feature-store", "monitoring"],
) as health_dag:

    validate_all = BashOperator(
        task_id="validate_all_views",
        bash_command=f"""
            {CLI_PATH} validate \\
                --name=all \\
                --check-storage \\
                --registry={REGISTRY_PATH}
        """,
        env={
            "FEATUREDUCK_STORAGE": STORAGE_PATH,
        },
    )

    check_runs = BashOperator(
        task_id="check_recent_runs",
        bash_command=f"""
            {CLI_PATH} runs \\
                --limit=20 \\
                --status=failed \\
                --output=json \\
                --registry={REGISTRY_PATH}
        """,
    )

    @task
    def alert_on_failures(**context):
        """Alert if there are recent failures."""
        ti = context["ti"]
        runs_output = ti.xcom_pull(task_ids="check_recent_runs")
        # Implement alerting logic here (Slack, PagerDuty, etc.)
        pass

    validate_all >> check_runs >> alert_on_failures()
