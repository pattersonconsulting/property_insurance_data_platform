from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

import pandas as pd
import json
import boto3
from boto3 import client
import pandas as pd
from io import StringIO

import os 

#import datetime
from airflow.sdk import DAG


os.environ['NO_PROXY'] = '*' 

sql_endpoint_name = "/sql/1.0/warehouses/17550ddda2a96e4e"


bucket = 'property-insurance-examples'
subdirectory = 'test-bordereaux-data/incoming_tpa_raw_data/'  

schema_conf_file = "company_schema_map.json"
#data_company_schemas = json.load( open(file_path, 'r') )



def list_subdirectories(bucket_name, prefix):
    """
    List subdirectories of a given prefix (subdirectory) in an S3 bucket.
    
    Args:
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The prefix path of the parent directory, must end with '/'.
    
    Returns:
        list of subdirectory names (str).
    """
    s3 = boto3.client('s3')
    
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix,
        Delimiter='/'
    )
    
    subdirs_full = []
    subdirs = []
    for cp in response.get('CommonPrefixes', []):
        parts = cp.get('Prefix').split("/")
        #print(parts[2])
        subdirs_full.append(cp.get('Prefix'))
        subdirs.append(parts[2])
    
    return subdirs, subdirs_full


def write_df_to_s3_csv(df, bucket_name, s3_key):
    s3_client = boto3.client('s3')
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())



def load_s3_file_as_text(bucket_name, file_key):

    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read().decode('utf-8')

        return file_content

    except Exception as e:
        print(f"Error loading file: {e}")
        return None


def load_s3_file_to_dataframe(bucket_name, file_key):

    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read().decode('utf-8')

        # Use StringIO to treat the string as a file
        csv_file = StringIO(file_content)

        df = pd.read_csv(csv_file)
        return df

    except Exception as e:
        print(f"Error loading file: {e}")
        return None


def list_files(bucket_name, prefix):

    s3 = boto3.client('s3')

    #print("list files: " + bucket_name + " : " + prefix)
    
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix,
        Delimiter='/'
    )


    rsp = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")


    file_list = []

    for obj in rsp["Contents"]:

        if obj["Key"].endswith(".csv"):

            parts = obj["Key"].split("/")

            file_list.append( parts[-1] )
    

    
    return file_list



def merge_incoming_s3_tpa_schemas():

    schema_conf_file_string = load_s3_file_as_text(bucket, subdirectory + schema_conf_file)

    data_company_schemas = json.loads( schema_conf_file_string )


    subdirs, subdirs_full = list_subdirectories(bucket, subdirectory)

    tpa_converted_df_list = []

    print("Processing Subdirectories:")

    for subdir, full_subdir in zip(subdirs, subdirs_full):

        print(full_subdir)

        file_list_subdir = list_files(bucket, full_subdir)


        for filename in file_list_subdir:

            print("\t" + filename)

            df_tmp = load_s3_file_to_dataframe(bucket, full_subdir + filename)

            print("Convert schema to standard internal schema: " + filename)

            df_new_claims_cols = df_tmp.rename(columns=data_company_schemas[subdir]["schema"])

            df_new_claims_cols['policy_start_date'] = pd.to_datetime(df_new_claims_cols['policy_start_date'], format=data_company_schemas[subdir]["date_format"])
            df_new_claims_cols['date_of_loss'] = pd.to_datetime(df_new_claims_cols['date_of_loss'], format=data_company_schemas[subdir]["date_format"])
            df_new_claims_cols['date_reported'] = pd.to_datetime(df_new_claims_cols['date_reported'], format=data_company_schemas[subdir]["date_format"])


            tpa_converted_df_list.append( df_new_claims_cols )

    df_all_bordereaux = pd.concat(tpa_converted_df_list)

    s3_key_write_merged_data = 'test-bordereaux-data/daily_bordereaux_merged_data/20250422_daily_merged.csv'
    write_df_to_s3_csv(df_all_bordereaux, bucket, s3_key_write_merged_data)



with DAG(
    'dbx_merge_incoming_tpa_schemas',
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),

    },
    description="Takes the incoming bordereaux data in different schemas in S3 and converts them to a single schema. Also merges them into a single csv.",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["s3", "bordereaux", "bronze"],

) as dag:
    task_read_file = PythonOperator(
        task_id='merge_tpa_schemas_python_task',
        python_callable=merge_incoming_s3_tpa_schemas
    )


        
