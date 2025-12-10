# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# from datetime import datetime

# default_args = {
#     "owner": "diganta",
#     "start_date": datetime(2024, 1, 1),
#     "retries": 1,
# }

# with DAG(
#     "bronze_ingestion_pipeline",
#     default_args=default_args,
#     schedule_interval="@daily",
#     catchup=False,
#     description="Extract -> Kafka -> Loader -> Iceberg -> Trino validation",
# ) as dag:

#     # Step 1: extract and push to kafka
#     run_connector = BashOperator(
#         task_id="run_connector_service",
#         bash_command="docker exec connector_service python3 /app/main.py",
#     )

#     # Step 2: loader is already continuously running as a service
#     # -> but we wait a few seconds for ingestion completion
#     wait_for_loader = BashOperator(
#         task_id="wait_loader",
#         bash_command="sleep 20"
#     )

#     # Step 3: Validate Iceberg table count using Trino
#     validate_iceberg = SQLExecuteQueryOperator(
#         task_id="validate_employee_table",
#         conn_id="trino_conn",
#         sql="SELECT COUNT(*) AS total_records FROM iceberg.default.employees",
#     )

#     run_connector >> wait_for_loader >> validate_iceberg


# from airflow import DAG
# from airflow.providers.docker.operators.docker import DockerOperator
# from datetime import datetime

# with DAG(
#     dag_id="bronze_ingestion_pipeline",
#     start_date=datetime(2024, 1, 1),
#     schedule_interval=None,
#     catchup=False
# ):

#     run_connector = DockerOperator(
#         task_id="run_connector",
#         image="postgres-minio-iceberg-trino-connector",
#         container_name="connector_service",
#         command="python3 /app/main.py",
#         docker_url="unix://var/run/docker.sock",
#         network_mode="postgres-minio-iceberg-trino_default",
#         auto_remove=False,
#         mount_tmp_dir=False
#     )

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_ingestion_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    description="Extract -> Kafka -> Loader -> Iceberg -> Trino validation",
):

    # Step 1: Run connector inside running container (Kafka producer)
    run_connector = BashOperator(
        task_id="run_connector",
        bash_command="docker exec connector_service python3 /app/main.py",
    )

    run_loader = BashOperator(
        task_id="run_loader",
        bash_command="docker exec loader_service python3 /app/loader/kafka_consumer.py"
    )

    #Step 2: Wait a few secs so loader can pick Kafka messages
    wait_loader = BashOperator(
        task_id="wait_for_loader",
        bash_command="sleep 20"
    )

    #Step 3: Validate Iceberg row count using Trino
    validate_iceberg = SQLExecuteQueryOperator(
        task_id="validate_iceberg_data",
        conn_id="trino_conn",
        sql="""
            SELECT COUNT(*) AS total_records
            FROM iceberg.raw.sales
        """,
        autocommit=True,
    )

    run_connector >> wait_loader >> validate_iceberg

