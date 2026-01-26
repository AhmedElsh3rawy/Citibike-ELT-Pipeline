from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

default_args = {"owner": "ahmed", "retries": 5, "retry_delay": timedelta(minutes=1)}

dag = DAG(
    dag_id="transformation_dag",
    default_args=default_args,
    description="A DAG for transforming and ingesting data",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
)

with dag:
    task1 = DockerOperator(
        task_id="transform_data",
        image="ghcr.io/dbt-labs/dbt-postgres:1.9.0",
        command="run",
        working_dir="/usr/app/citibike_project",
        mounts=[
            Mount(
                source="/home/ahmed/Programming/github/Citibike-ELT-Pipeline/dbt",
                target="/usr/app",
                type="bind",
            ),
            Mount(
                source="/home/ahmed/Programming/github/Citibike-ELT-Pipeline/dbt",
                target="/root/.dbt",
                type="bind",
            ),
        ],
        network_mode="citibike-elt-pipeline_citibike-net",
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
    )
