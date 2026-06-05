from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/opt/airflow/zeco-platform"

default_args = {
    "owner": "zeco-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="zeco_revenue_protection_pipeline",
    default_args=default_args,
    description="Orchestrates ZECO ETL, revenue protection, and forecasting jobs",
    schedule_interval="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["zeco", "spark", "revenue-protection"],
) as dag:

    revenue_protection_anomaly = BashOperator(
        task_id="run_revenue_protection_anomaly",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python src/ml/revenue_protection_anomaly.py"
        ),
    )

    arima_forecast = BashOperator(
        task_id="run_arima_revenue_forecast",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python src/ml/arima_forecast.py"
        ),
    )

    prophet_forecast = BashOperator(
        task_id="run_prophet_consumption_forecast",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python src/ml/prophet_token_forecast.py"
        ),
    )

    revenue_protection_anomaly >> arima_forecast >> prophet_forecast