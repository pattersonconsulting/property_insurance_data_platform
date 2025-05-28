import pandas as pd
import json
import boto3

def list_folders_in_bucket(bucket_name):
    # Create a Boto3 client for S3
    s3_client = boto3.client('s3')
 
    # Use the list_objects_v2 method to get the list of objects in the bucket
    response = s3_client.list_objects_v2(Bucket=bucket_name, Delimiter='/')
 
    # Check if the bucket has prefixes (folders)
    if 'CommonPrefixes' in response:
        # Loop through the prefixes and print their names (folder names)
        for folder in response['CommonPrefixes']:
            print(folder['Prefix'])
    else:
        print("Bucket is empty or contains no folders.")


data = {
  "calories": [420, 380, 390],
  "duration": [50, 40, 45]
}

#load data into a DataFrame object:
df = pd.DataFrame(data)

cols = {
		"calories": "foo",	
		"duration": "bar"
	}	


print(df)

# df.rename(columns={"data.datasets":"mydata"})

df_new = df.rename(columns=cols)

print(df_new)


df_claims = pd.read_csv('claims-company-a.csv')

print(df_claims)


file_path = "company_schema_map.json"


data = json.load( open(file_path, 'r') )

#print(data)

#company_conf = data.get("company-a", None)

#print("Found:")
#print(data["company-a"])

df_new_claims_cols = df_claims.rename(columns=data["company-a"])

print(df_new_claims_cols)


print("checking s3 buckets")

from boto3 import client

conn = client('s3')  # again assumes boto.cfg setup, assume AWS S3

for key in conn.list_objects(Bucket='property-insurance-examples')['Contents']:

    print(key['Key'])

print("now just sub folders:")    

bucket = 'property-insurance-examples'
#Make sure you provide / in the end
subdirectory = 'test-bordereaux-data/incoming_tpa_raw_data/'  

import boto3

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
    
    subdirs = []
    for cp in response.get('CommonPrefixes', []):
        parts = cp.get('Prefix').split("/")
        print(parts[2])
        subdirs.append(cp.get('Prefix'))
    
    return subdirs


subdirs = list_subdirectories(bucket, subdirectory)

print("Subdirectories:")
for subdir in subdirs:
    print(subdir)


