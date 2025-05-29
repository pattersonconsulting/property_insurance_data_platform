import pandas as pd
import json
import boto3
from boto3 import client
import pandas as pd
from io import StringIO

file_path = "company_schema_map.json"


data_company_schemas = json.load( open(file_path, 'r') )


#df_new_claims_cols = df_claims.rename(columns=data["company-a"])

#print(df_new_claims_cols)


print("checking s3 buckets")


'''
conn = client('s3')  # again assumes boto.cfg setup, assume AWS S3

for key in conn.list_objects(Bucket='property-insurance-examples')['Contents']:

    print(key['Key'])

print("now just sub folders:")    

import boto3
'''

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




def load_s3_file_to_dataframe(bucket_name, file_key):
    """Loads a file from S3 into a Pandas DataFrame.

    Args:
        bucket_name (str): The name of the S3 bucket.
        file_key (str): The key (path) of the file in the S3 bucket.

    Returns:
        pandas.DataFrame: The DataFrame containing the data from the file.
        Returns None if there's an error.
    """

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

    #s3 = boto3.client("s3")

    rsp = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")

    #print("Objects:", list(obj["Key"] for obj in rsp["Contents"]))

    file_list = []

    for obj in rsp["Contents"]:

        if obj["Key"].endswith(".csv"):

            parts = obj["Key"].split("/")

            file_list.append( parts[-1] )
    
    
    
    '''
    for cp in response.get('CommonPrefixes', []):
        parts = cp.get('Prefix').split("/")
        #print(parts[2])
        print(cp)
        #subdirs_full.append(cp.get('Prefix'))
        file_list.append(parts[3])
    '''
    
    return file_list

bucket = 'property-insurance-examples'
#Make sure you provide / in the end
subdirectory = 'test-bordereaux-data/incoming_tpa_raw_data/'  


subdirs, subdirs_full = list_subdirectories(bucket, subdirectory)

tpa_converted_df_list = []

print("Processing Subdirectories:")

for subdir, full_subdir in zip(subdirs, subdirs_full):


    #print(subdir)
    print(full_subdir)

    file_list_subdir = list_files(bucket, full_subdir)

    #data_company_schemas

    

    for filename in file_list_subdir:

        print("\t" + filename)

        

        df_tmp = load_s3_file_to_dataframe(bucket, full_subdir + filename)

        #print(df_tmp)

        print("Convert schema to standard internal schema: " + filename)

        df_new_claims_cols = df_tmp.rename(columns=data_company_schemas[subdir]["schema"])

        df_new_claims_cols['policy_start_date'] = pd.to_datetime(df_new_claims_cols['policy_start_date'], format=data_company_schemas[subdir]["date_format"])
        df_new_claims_cols['date_of_loss'] = pd.to_datetime(df_new_claims_cols['date_of_loss'], format=data_company_schemas[subdir]["date_format"])
        df_new_claims_cols['date_reported'] = pd.to_datetime(df_new_claims_cols['date_reported'], format=data_company_schemas[subdir]["date_format"])


        #print(df_new_claims_cols)

        tpa_converted_df_list.append( df_new_claims_cols )

df_all_bordereaux = pd.concat(tpa_converted_df_list)

# policy_start_date,date_of_loss,date_reported

print(df_all_bordereaux)


# s3.upload_file('local_file.txt', 'my-bucket', 'object_name.txt')

s3_key_write_merged_data = 'test-bordereaux-data/daily_bordereaux_merged_data/20250422_daily_merged.csv'
write_df_to_s3_csv(df_all_bordereaux, bucket, s3_key_write_merged_data)

