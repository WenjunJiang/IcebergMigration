# create_iceberg_sqlite_example.py
# Requires: pip install "pyiceberg[sql-sqlite]" pyarrow

from pathlib import Path
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType, DoubleType
import pyarrow as pa

# --- Paths from your snippet ---
uri = "sqlite:////Users/jwj/data/iceberg/old/iceberg.db"
warehouse = "file:///Users/jwj/data/iceberg/old/warehouse"
TABLE_IDENT = ("local", "my_table")
TABLE_NAME_STR = ".".join(TABLE_IDENT)

# Ensure folders exist
Path("/Users/jwj/data/iceberg/old").mkdir(parents=True, exist_ok=True)
Path("/Users/jwj/data/iceberg/old/warehouse").mkdir(parents=True, exist_ok=True)

# Load a SQLite-backed catalog and create the namespace
catalog = load_catalog("default", type="sql", uri=uri, warehouse=warehouse)
try:
    catalog.create_namespace(TABLE_IDENT[0])
except Exception:
    # ok if it already exists
    pass

# Define a simple schema (field IDs are required by Iceberg)
schema = Schema(
    NestedField(1, "id", IntegerType(), required=True),
    NestedField(2, "city", StringType(), required=False),
    NestedField(3, "lat", DoubleType(), required=False),
    NestedField(4, "long", DoubleType(), required=False),
)

# Create the table if it doesn't yet exist (will place it under the warehouse path)
table = catalog.create_table_if_not_exists(TABLE_NAME_STR, schema=schema)

# Prepare a small Arrow table and append some rows
data = [
    {"id": 1, "city": "San Francisco", "lat": 37.773972, "long": -122.431297},
    {"id": 2, "city": "Amsterdam",     "lat": 52.371807, "long": 4.896029},
    {"id": 3, "city": "Paris",         "lat": 48.864716, "long": 2.349014},
]
arrow_tbl = pa.Table.from_pylist(data, schema=table.schema().as_arrow())

# Write data into Iceberg (creates metadata + parquet under the warehouse)
table.append(arrow_tbl)   # or: table.overwrite(arrow_tbl)

# Quick check: read back into Arrow and print
print(table.scan().to_arrow().to_string(preview_cols=10))
print(f"\nCreated catalog at: {uri}")
print(f"Warehouse root: {warehouse}")
print(f"Table id: {TABLE_NAME_STR}")
