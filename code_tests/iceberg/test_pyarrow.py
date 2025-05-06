import pyarrow.csv as pc
import pyarrow as pa

# Read the CSV file
table = pc.read_csv('./data/ZoneToFIPS.csv')

'''
# If you have a file-like object instead of a file path
with open('your_file.csv', 'rb') as f:
    table_from_buffer = pc.read_csv(f)
'''

# Print the table schema
print(table.schema)

# Convert to pandas DataFrame (if needed)
df = table.to_pandas()
print(df.head())