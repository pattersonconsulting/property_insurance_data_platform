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

'''
catalog.create_namespace("default")

table = catalog.create_table(
    "default.fips_dataset",
    schema=py_arrow_table.schema,
)
'''

py_arrow_table = catalog.load_table("default.fips_dataset")

print(py_arrow_table)


#pandas_df = py_arrow_table.to_pandas()
pandas_df = py_arrow_table.scan().to_pandas()

print(pandas_df)