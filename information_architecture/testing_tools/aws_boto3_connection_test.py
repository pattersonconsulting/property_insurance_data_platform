import boto3

'''
ec2 = boto3.client('ec2')
response = ec2.describe_instances()
print(response)
'''

'''
print("listing all rds dbs...")

available_regions = boto3.Session().get_available_regions('rds')

for region in available_regions:
    rds = boto3.client('rds', region_name=region)
    paginator = rds.get_paginator('describe_db_instances').paginate()
    for page in paginator:
        for dbinstance in page['DBInstances']:
            print("{DBInstanceClass}".format(**dbinstance))

'''

output = boto3.client('sts').get_caller_identity().get('Account')

print("out: " + str(output))

'''
client = boto3.client('s3tables')

response = client.list_tables(
    tableBucketARN='claims_data',
    namespace='claims')
'''

# sts_client = boto3.client('sts')
# account_id = sts_client.get_caller_identity().get('Account')

from boto3 import client

conn = client('s3')  # again assumes boto.cfg setup, assume AWS S3
for key in conn.list_objects(Bucket='s3tables-debug-logs')['Contents']:
    print(key['Key'])