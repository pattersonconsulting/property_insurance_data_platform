from pyiceberg.catalog import load_catalog

import pyarrow.csv as pc
import pyarrow as pa

# import pyarrow as py_df


warehouse_path = "./warehouse"
catalog = load_catalog(
    "default",
    **{
        'type': 'sql',
        "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
        "warehouse": f"file://{warehouse_path}",
    },
)



# Read the CSV file
py_arrow_table = pc.read_csv('./data/ZoneToFIPS.csv')

print("FIPS CSV Loaded...")

# Convert to pandas DataFrame (if needed)
pandas_df = py_arrow_table.to_pandas()

catalog.create_namespace("default")

table = catalog.create_table(
    "default.fips_dataset",
    schema=py_arrow_table.schema,
)

table.append(py_arrow_table)
#len(table.scan().to_arrow())

print("Table saved to iceberg...")