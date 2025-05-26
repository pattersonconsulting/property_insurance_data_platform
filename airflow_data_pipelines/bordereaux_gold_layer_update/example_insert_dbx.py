from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksCopyIntoOperator
from datetime import datetime

with DAG(
    dag_id="databricks_s3_to_unity_catalog",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["databricks", "s3", "unity_catalog"],
) as dag:
    copy_into_task = DatabricksCopyIntoOperator(
        task_id="copy_into_s3_to_uc",
        databricks_conn_id="databricks_default",  # Replace with your connection ID
        sql_endpoint_name="my_sql_endpoint",  # Replace with your SQL endpoint name
        table_name="catalog.schema.my_table",  # Replace with your Unity Catalog table name
        file_location="s3://your-s3-bucket/path/to/your/data.csv",  # Replace with your S3 path
        file_format="CSV",
        format_options={"header": "true"},
        force_copy=True,
    )