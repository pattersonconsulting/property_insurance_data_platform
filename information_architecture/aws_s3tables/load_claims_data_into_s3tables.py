
from pyiceberg.catalog import load_catalog
import pyarrow as pa
import pandas as pd
import boto3
import argparse
import json
import csv

from pyiceberg.exceptions import (
    CommitFailedException,
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchIcebergTableError,
    NoSuchNamespaceError,
    NoSuchPropertyException,
    NoSuchTableError,
    TableAlreadyExistsError,
)

# Constants
REGION = 'us-east-1'
CATALOG = 's3tablescatalog'
DATABASE = 'bordereaux_namespace'
TABLE_BUCKET = 'bordereaux-table-bucket'
TABLE_NAME = 'bordereaux_data'


print("Testing S3Tables from PyIceberg...")


def csv_to_list_of_dicts(file_path):
    """
    Reads a CSV file and returns a list of dictionaries.
    Each dictionary represents a row, with keys as column headers.
    """
    data = []
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)
    return data


def load_table(catalog, database, table_name):
    """Load an Iceberg table."""
    try:
        table = catalog.load_table(f"{database}.{table_name}")
        print(f"Table schema: {table.schema()}")
        return table
    except Exception as e:
        print(f"Error loading the table: {e}")
        return None

def get_aws_account_id():
    """Attempt to get account ID from STS."""
    try:
        sts_client = boto3.client('sts')
        account_id = sts_client.get_caller_identity().get('Account')
        return account_id
    except Exception as e:
        print(f"Error getting account ID: {e}")
        return None


def initialize_catalog(account_id):
    """Initialize catalog using the Glue Iceberg REST endpoint."""
    try:
        rest_catalog = load_catalog(
            CATALOG,
            **{
                "type": "rest",
                "warehouse": f"{account_id}:{CATALOG}/{TABLE_BUCKET}",
                "uri": f"https://glue.{REGION}.amazonaws.com/iceberg",
                "rest.sigv4-enabled": "true",
                "rest.signing-name": "glue",
                "rest.signing-region": REGION,
            },
        )
        print("Catalog loaded successfully!")
        return rest_catalog
    except Exception as e:
        print(f"Error loading catalog: {e}")
        return None


def convert_df_to_iceberg_schema(df, table_schema):


    for field in table_schema.fields:
            col_name = field.name
            col_type = field.field_type

            if col_name in df.columns:
                if str(col_type) == "long":
                    df[col_name] = df[col_name].astype(int)
                elif str(col_type) == "double":
                    df[col_name] = df[col_name].astype(float)
                elif str(col_type) == "boolean":
                     df[col_name] = df[col_name].astype(bool)
                elif str(col_type).startswith("decimal"):
                    precision, scale = map(int, str(col_type)[8:-1].split(','))
                    df[col_name] = df[col_name].astype(f"float")
                elif str(col_type).startswith("string"):
                    df[col_name] = df[col_name].astype(str) 
                    
                elif str(col_type).startswith("date"):
                    df[col_name] = df[col_name].astype('datetime64[s]')


print("Testing s3table catalog...")

account_id = get_aws_account_id()

# get a pyiceberg catalog
catalog = initialize_catalog(account_id)


#print( json.dumps(catalog, indent=4) )

print("Catalog namespaces:")

for n in catalog.list_namespaces():

    print(n)



print("list of tables:")

for n in catalog.list_tables(DATABASE):

    print(n)    


table_identifier = catalog.list_tables(DATABASE)[0] 

'''
print("convert identifier_to_database_and_table()")
database_name, table_name = catalog.identifier_to_database_and_table(table_identifier, NoSuchTableError)

print(table_name)
'''
print("load_table()")
#print( table_identifier )

#table = catalog.load_table( table_identifier )

table = load_table(catalog, "bordereaux_namespace", "bordereaux_data")

#print("Print table as dataframe:")
#df = table.to_pandas()

#print(df)

print("load claims.csv ...")
#data_dict = csv_to_list_of_dicts("./claims.csv")

df_claims = pd.read_csv('./claims.csv')

print( df_claims )

#table_schema_arrow = table.schema().as_arrow()

convert_df_to_iceberg_schema(df_claims, table.schema())


print("Converted Schema ---------- ")
print(df_claims.info())

#print( df_claims )



'''
for field in table.schema().fields:
    print(f"Column Name: {field.name}")
    print(f"Column ID: {field.field_id}")
    print(f"Column Type: {field.field_type}\n")
    #print(f"Is Required: {field.is_required}\n")
'''



#print(data_dict)

print("Schema...")
print(type(table.schema()))

table_schema_arrow = table.schema().as_arrow()

#print(type(table_schema_arrow))

print("PyArrow Table Conversion")
#df = pa.Table.from_pylist( data_dict, schema=table_schema_arrow )
updated_claims_s3table = pa.Table.from_pandas(df_claims, schema=table_schema_arrow)
#table.append(df)

print( updated_claims_s3table )

print("Loading data into AWS S3Table ...")

table.append(updated_claims_s3table)

print("Data update complete in AWS...")

