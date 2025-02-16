# pyiceberg-upsert-demo
pyiceberg-upsert-demo

![image](https://github.com/user-attachments/assets/4e6004f6-0c08-48e7-8881-289f03d70c78)

## Install 
```
pip install -e git+https://github.com/apache/iceberg-python.git@main#egg=pyiceberg

```

#### Sample code 
```
"""
pip install -e git+https://github.com/apache/iceberg-python.git@main#egg=pyiceberg


"""

import os
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform


# Set up the warehouse path (replace with your actual path)
warehouse_path = "/Users/soumilshah/IdeaProjects/icebergpython/tests/warehouse"
os.makedirs(warehouse_path, exist_ok=True)

# Set up the catalog
catalog = load_catalog(
    "hive",
    warehouse=warehouse_path,
    uri=f"sqlite:///{warehouse_path}/metastore.db"
)

# Define the schema
schema = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "site_id", StringType(), required=True),
    NestedField(3, "message", StringType(), required=True),
    identifier_field_ids=[1]  # 'id' is the primary key
)

# Define the partition spec (partition by 'site_id')
partition_spec = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="site_id")
)

# Create the namespace if it doesn't exist
namespace = "my_namespace"
if namespace not in catalog.list_namespaces():
    catalog.create_namespace(namespace)

# Create the table
table_name = f"{namespace}.site_messages"
try:
    table = catalog.create_table(
        identifier=table_name,
        schema=schema,
        partition_spec=partition_spec,
        location=f"{warehouse_path}/{namespace}/site_messages"
    )
    print(f"Created table: {table_name}")
except Exception as e:
    print(f"Table already exists: {e}")
    table = catalog.load_table(table_name)

# Create initial sample data
initial_data = pa.table([
    pa.array([1, 2]),
    pa.array(["site_1", "site_2"]),
    pa.array(["initial message 1", "initial message 2"])
], schema=schema.as_arrow())

# Write initial data to the table
table.append(initial_data)
print("Initial data written to the table")

# Read and print initial data
initial_df = table.scan().to_arrow().to_pandas()
print("\nInitial data in the table:")
print(initial_df)

# Create data for upsert
upsert_data = pa.table([
    pa.array([2, 3]),  # Update id=2, insert id=3
    pa.array(["site_2", "site_3"]),
    pa.array(["updated message 2", "initial message 3"])
], schema=schema.as_arrow())

# Construct boolean expression for merge condition
join_columns = ["id"]
# Perform the merge operation
# Perform the upsert operation
upsert_result = table.upsert(df=upsert_data, join_cols = join_columns)

print("\nUpsert operation completed")
print(f"Rows Updated: {upsert_result.rows_updated}")
print(f"Rows Inserted: {upsert_result.rows_inserted}")

# Read and print the updated data
updated_df = table.scan().to_arrow().to_pandas()
print("\nUpdated data in the table after upsert operation:")
print(updated_df)

```
