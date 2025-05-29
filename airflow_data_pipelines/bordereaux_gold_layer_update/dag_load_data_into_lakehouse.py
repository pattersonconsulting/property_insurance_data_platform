from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

import os 

#import datetime
from airflow.sdk import DAG

from airflow.providers.databricks.operators.databricks_sql import (
    DatabricksCopyIntoOperator,
    DatabricksSqlOperator,
)

os.environ['NO_PROXY'] = '*' 

sql_endpoint_name = "/sql/1.0/warehouses/17550ddda2a96e4e"


with DAG(
    'dbx_connection_text',
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),

    },
    description="Loads data into Databricks Delta Lake Table from S3",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],

) as dag:
    test_dbx_data_task = DatabricksSqlOperator(
        task_id="test_dbx_connection_task",
        
        
        databricks_conn_id="databricks_connection",
        http_path=sql_endpoint_name,

        sql="load_s3_bordereaux_into_dbx_airflow.sql"

    )

        
