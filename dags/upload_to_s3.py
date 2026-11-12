import os
import glob
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


PROJECT_DIR = "/opt/airflow/project_1"
SCRIPT_NAME = "spark_mart.py"
# Укажите имя папки, которую создает Spark (или оставьте PROJECT_DIR для поиска везде)
SPARK_OUTPUT_DIR = "/opt/airflow/project_1/csv_files" 

BUCKET_NAME = "spark-marts"
S3_CONN_ID = "minio_s3"

local_tz = pendulum.timezone("Europe/Moscow")

def upload_spark_csv_to_minio(**context):
    hook = S3Hook(aws_conn_id=S3_CONN_ID)
    
    if not hook.check_for_bucket(BUCKET_NAME):
        hook.create_bucket(bucket_name=BUCKET_NAME)

    # Ищем только файлы данных Spark (part-*.csv), игнорируя _SUCCESS и .crc
    search_pattern = os.path.join(SPARK_OUTPUT_DIR, "**", "part-*.csv")
    csv_files = glob.glob(search_pattern, recursive=True)
    
    if not csv_files:
        raise FileNotFoundError(f"Файлы part-*.csv не найдены в {SPARK_OUTPUT_DIR}")

    execution_date = context["ds"]

    for file_path in csv_files:
        # Сохраняем структуру папки (например: 2026-11-12/mart_output/part-00000.csv)
        relative_path = os.path.relpath(file_path, PROJECT_DIR)
        s3_key = f"{execution_date}/{relative_path}".replace("\\", "/")

        hook.load_file(
            filename=file_path,
            key=s3_key,
            bucket_name=BUCKET_NAME,
            replace=True
        )
        print(f"Загружен партиционированный файл: {file_path} -> s3://{BUCKET_NAME}/{s3_key}")

default_args = {
    "owner": "maxim",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spark_clickhouse_to_minio",
    default_args=default_args,
    description="Чтение из ClickHouse через Spark и выгрузка CSV в MinIO в 10:00 МСК",
    schedule="0 10 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz=local_tz),
    catchup=False,
    tags=["spark", "clickhouse", "minio"],
) as dag:

    run_spark_script = BashOperator(
        task_id="run_spark_mart",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"spark-submit --master local[*] --jars $(echo {PROJECT_DIR}/jars/*.jar | tr ' ' ',') {SCRIPT_NAME}"
        ),
    )


    upload_to_s3 = PythonOperator(
        task_id="upload_spark_csv",
        python_callable=upload_spark_csv_to_minio,
    )

    run_spark_script >> upload_to_s3
