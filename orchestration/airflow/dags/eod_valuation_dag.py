
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

SPARK_IMAGE = "lamf/spark-iceberg:local"
NETWORK = "lamf_default"
SUBMIT = "/opt/spark/bin/spark-submit"

COMMON = dict(
    image=SPARK_IMAGE,
    network_mode=NETWORK,
    auto_remove=True,
    mount_tmp_dir=False,
    mounts=[
        Mount(source="lamf_repo_spark", target="/opt/jobs/spark", type="volume"),
    ],
    environment={
        "AWS_ACCESS_KEY_ID": "admin",
        "AWS_SECRET_ACCESS_KEY": "password123",
        "AWS_REGION": "us-east-1",
    },
)

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="eod_valuation",
    description="LAMF end-of-day medallion valuation pipeline",
    schedule="0 2 * * *",        
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["lamf", "batch", "medallion"],
) as dag:

    def spark_task(task_id: str, script: str) -> DockerOperator:
        return DockerOperator(
            task_id=task_id,
            command=f"{SUBMIT} /opt/jobs/spark/{script}",
            **COMMON,
        )

    land_bronze = spark_task("land_bronze", "bronze/land_seed.py")
    build_silver = spark_task("build_silver", "silver/build_silver.py")
    build_gold = spark_task("build_gold", "gold/build_gold.py")

    ge_silver = DockerOperator(
        task_id="ge_silver",
        command="python /opt/jobs/quality/great_expectations/run_checkpoint.py silver",
        **COMMON,
    )
    ge_gold = DockerOperator(
        task_id="ge_gold",
        command="python /opt/jobs/quality/great_expectations/run_checkpoint.py gold",
        **COMMON,
    )

    land_bronze >> build_silver >> ge_silver >> build_gold >> ge_gold
