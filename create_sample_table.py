import os
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, StringType, DoubleType, NestedField # Corrected import

# Define the schema for the Iceberg table
schema = Schema(
    NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
    NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    NestedField(field_id=3, name="price", field_type=DoubleType(), required=False),
    NestedField(field_id=4, name="category", field_type=StringType(), required=False)
)

# Sample data
data1 = {
    "id": [1, 2, 3],
    "name": ["Laptop", "Mouse", "Keyboard"],
    "price": [1200.00, 25.00, 75.00],
    "category": ["Electronics", "Electronics", "Electronics"]
}
df1 = pd.DataFrame(data1)
arrow_table1 = pa.Table.from_pandas(df1, schema=schema.as_arrow())


data2 = {
    "id": [4, 5],
    "name": ["Desk", "Chair"],
    "price": [250.00, 150.00],
    "category": ["Furniture", "Furniture"]
}
df2 = pd.DataFrame(data2)
arrow_table2 = pa.Table.from_pandas(df2, schema=schema.as_arrow())

data3 = {
    "id": [6, 7, 8, 9],
    "name": ["Monitor", "Webcam", "Headphones", "Desk Lamp"],
    "price": [300.00, 50.00, 100.00, 30.00],
    "category": ["Electronics", "Electronics", "Electronics", "Office Supplies"]
}
df3 = pd.DataFrame(data3)
arrow_table3 = pa.Table.from_pandas(df3, schema=schema.as_arrow())

catalog_name = "local_catalog" # This is the namespace in PyIceberg
table_name = "sample_table"
table_identifier = f"{catalog_name}.{table_name}"

warehouse_path = os.path.join(os.getcwd(), "sample_iceberg_table_warehouse")
if not os.path.exists(warehouse_path):
    os.makedirs(warehouse_path)

# Using an in-memory catalog that writes to a local directory (requires sqlalchemy for this type)
# For pyiceberg 0.9.1, 'in-memory' with a URI seems to be a way to get local file storage.
# The URI needs to point to the SQLite DB file for the catalog.
db_path = os.path.join(warehouse_path, "pyiceberg_catalog.db")
conf = {
    "type": "in-memory",
    "uri": f"sqlite:///{db_path}", # URI for SQLAlchemy
    "warehouse": warehouse_path,
}

catalog = load_catalog(
    "local_pyiceberg_catalog",
    **conf
)

# Create namespace if it doesn't exist
try:
    catalog.create_namespace(namespace=catalog_name)
    print(f"Namespace '{catalog_name}' created.")
except Exception as e:
    print(f"Namespace '{catalog_name}' likely already exists or other error: {e}")


# Create the table or load if it exists
try:
    table = catalog.load_table(table_identifier)
    print(f"Table {table_identifier} already exists. Appending data.")
except Exception:
    print(f"Table {table_identifier} does not exist. Creating now.")
    table = catalog.create_table(identifier=table_identifier, schema=schema)
    print(f"Table {table_identifier} created successfully.")

# Append data to the table
print("Appending first Arrow Table...")
table.append(arrow_table1)
print("First Arrow Table appended.")

print("Appending second Arrow Table...")
table.append(arrow_table2)
print("Second Arrow Table appended.")

print("Appending third Arrow Table...")
table.append(arrow_table3)
print("Third Arrow Table appended.")

print(f"Sample Iceberg table created/updated. Check warehouse: {warehouse_path}")
print(f"Table base location should be: {warehouse_path}/{catalog_name}/{table_name}")

# Simplified verification: Check current snapshot
table = catalog.load_table(table_identifier)
if table.current_snapshot():
    print(f"Current snapshot ID: {table.current_snapshot().snapshot_id}")
    print(f"Manifest list for current snapshot: {table.current_snapshot().manifest_list}")
else:
    print("No current snapshot found for the table.")

print(f"Number of snapshots in history: {len(table.history())}")
for entry in table.history():
    print(f"  Snapshot Log Entry: Snapshot ID: {entry.snapshot_id}, Timestamp: {entry.timestamp_ms}")

print("Script completed.")
