#from airflow.operators.python import PythonOperator
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

def my_python_function(ti):

    #os.environ["no_proxy"]="*"

    print("Hello world!")
    return "Task executed successfully"

with DAG(
    'dbx_copy_load_file',
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'on_skipped_callback': another_function, #or list of functions
        # 'trigger_rule': 'all_success'
    },
    description="Testing dbx table load functionality",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],

) as dag:
'''
    task = PythonOperator(
        task_id='my_python_task',
        python_callable=my_python_function
        #op_kwargs={'ti': ti}  # Pass TaskInstance to the function
    )
'''

    load_dbx_data_task = DatabricksCopyIntoOperator(
        task_id="load_dbx_data_task",
        #expression_list="date::date, * except(date)",
        
        databricks_conn_id=connection_id,
        sql_endpoint_name=sql_endpoint_name,
        table_name="my_table",
        file_format="CSV",
        file_location="abfss://container@account.dfs.core.windows.net/my-data/csv",
        format_options={"header": "true"},
        force_copy=True,
    )

        
