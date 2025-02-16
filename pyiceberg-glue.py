import os
import pyarrow as pa
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.expressions import EqualTo
from pyiceberg.exceptions import NoSuchTableError
from tabulate import tabulate


def setup_glue_catalog(warehouse_path):
    return GlueCatalog("glue_catalog", **{
        "warehouse": warehouse_path,
        "region": "us-east-1"  # Replace with your AWS region
    })


def create_schema():
    return Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "site_id", StringType(), required=True),
        NestedField(3, "message", StringType(), required=True),
        identifier_field_ids=[1]  # 'id' is the primary key
    )


def create_partition_spec():
    return PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="site_id")
    )


def create_or_load_table(catalog, namespace, table_name, schema, partition_spec, warehouse_path):
    if namespace not in catalog.list_namespaces():
        try:
            catalog.create_namespace(namespace)
            print(f"Created namespace: {namespace}")
        except Exception as e:
            print(f"Namespace already exists: {e}")

    full_table_name = f"{namespace}.{table_name}"
    try:
        table = catalog.load_table(full_table_name)
        print(f"Loaded existing table: {full_table_name}")
    except NoSuchTableError:
        print(f"Table {full_table_name} does not exist. Creating it...")
        table = catalog.create_table(
            identifier=full_table_name,
            schema=schema,
            partition_spec=partition_spec,
            location=f"{warehouse_path}/{namespace}/{table_name}"
        )
        print(f"Created table: {full_table_name}")
    return table


def overwrite_data(table, df):
    print("\n--- OVERWRITE OPERATION ---")
    print("Overwriting data in the Iceberg table...")
    table.overwrite(df)


def delete_data(table, delete_filter):
    print("\n--- DELETE OPERATION ---")
    print(f"Deleting data where {delete_filter}...")
    table.delete(delete_filter)


def append_data(table, df):
    print("\n--- APPEND OPERATION ---")
    print("Appending data to the Iceberg table...")
    table.append(df)


def upsert_data(table, df, join_cols):
    print("\n--- UPSERT OPERATION ---")
    print("Performing upsert operation on the Iceberg table...")
    result = table.upsert(df=df, join_cols=join_cols)
    print(f"Rows updated: {result.rows_updated}, rows inserted: {result.rows_inserted}")


def read_data(table):
    return table.scan().to_arrow().to_pandas()


def create_sample_data(schema, data):
    return pa.table(data, schema=schema.as_arrow())


def print_table(df, title):
    print(f"\n{title}")
    print(tabulate(df, headers='keys', tablefmt='pretty', showindex=False))
    print("\n")


def main():
    warehouse_path = 's3://XX/pyiceberg'
    catalog = setup_glue_catalog(warehouse_path)
    schema = create_schema()
    partition_spec = create_partition_spec()
    table = create_or_load_table(catalog, "icebergdb", "site_messages", schema, partition_spec, warehouse_path)

    # Overwrite Operation
    initial_data = create_sample_data(schema, [
        pa.array([1, 2, 3]),
        pa.array(["site_1", "site_2", "site_3"]),
        pa.array(["initial message 1", "initial message 2", "initial message 3"])
    ])
    overwrite_data(table, initial_data)
    print_table(read_data(table), "Data after Overwrite:")

    # Append Operation
    append_data_pa = create_sample_data(schema, [
        pa.array([4, 5]),
        pa.array(["site_4", "site_5"]),
        pa.array(["appended message 4", "appended message 5"])
    ])
    append_data(table, append_data_pa)
    print_table(read_data(table), "Data after Append:")

    # Delete Operation
    delete_data(table, EqualTo('site_id', 'site_1'))
    print_table(read_data(table), "Data after Deletion:")

    # Upsert Operation
    upsert_data_pa = create_sample_data(schema, [
        pa.array([2, 6]),  # Update id=2, insert id=6
        pa.array(["site_2", "site_6"]),
        pa.array(["updated message 2", "new message 6"])
    ])
    upsert_data(table, upsert_data_pa, ["id"])
    print_table(read_data(table), "Final Data after Upsert:")


if __name__ == "__main__":
    main()
